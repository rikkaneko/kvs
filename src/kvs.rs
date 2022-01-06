/*
 * This file is part of kvs.
 * Copyright (c) 2021 Joe Ma <rikkaneko23@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#![allow(clippy::collapsible_match)]
use std::cell::RefCell;
use std::cmp::max;
use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use thiserror::Error;
pub type Result<T> = std::result::Result<T, KvError>;

const KV_BUILD_NUMBER: u64 = 1000;
const KV_MIN_COMPACTION_THRESHOLD: u64 = 8192;

#[derive(Debug)]
pub struct KvStore {
	header: KvHeader,
	file_db_handle: File,
	file_index_handle: File,
	index: HashMap<String, u64>,
	is_compaction: bool,
	modified: bool
}

pub struct KvsServer {
	store: Arc<RefCell<dyn KvsEngine>>
}

/// `KvsEngine` Defines the storage interface called by KvsServer
pub trait KvsEngine {
	/// Get the string value of a given string key
	fn set(&mut self, key: String, value: String) -> Result<()>;
	/// Get the string value of a given string key
	fn get(&self, key: String) -> Result<Option<String>>;
	/// Remove a given key `key`
	fn remove(&mut self, key: String) -> Result<()>;
	/// Create or open KvStore instance
	fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
}

#[derive(Error, Debug)]
pub enum KvError {
	#[error(transparent)]
	IOError(#[from] std::io::Error),
	#[error(r#"Key "{0}" does not exist"#)]
	KeyNotExist(String),
	#[error("Invalid index detected. The database file has been modified externally")]
	InvalidIndex,
	#[error(transparent)]
	SerializationError(#[from] bson::ser::Error),
	#[error(transparent)]
	DeserializationError(#[from] bson::de::Error),
	#[error("Unsupported Engine")]
	UnsupportedEngine,
	#[error("Cannot open the database file")]
	InvalidDatabaseFormat,
	#[error("Found incompatible database version {0}, current version {}")]
	IncompatibleDatabaseVersion(u64, u64),
	#[error(transparent)]
	SystemTimeError(#[from] std::time::SystemTimeError)
}

#[derive(Serialize, Deserialize, Debug)]
enum KvEntries {
	SET(String, String),
	DELETE(String)
}

#[derive(Serialize, Deserialize, Debug)]
struct KvIndexEntries {
	key: String,
	offset: u64
}

#[derive(Serialize, Deserialize, Debug)]
struct KvHeader {
	build_number: u64,
	last_open: u64,
	next_compaction_size: u64 // in byte
}

impl KvsEngine for KvStore {
	/// Set the value of a string key to a string
	fn set(&mut self, key: String, value: String) -> Result<()> {
		self.writeback(KvEntries::SET(key, value))?;
		Ok(())
	}
	
	/// Get the string value of a given string key
	fn get(&self, key: String) -> Result<Option<String>> {
		match self.index.get(&key) {
			Some(value) => {
				let mut reader = BufReader::new(self.file_db_handle.try_clone()?);
				reader.seek(SeekFrom::Start(*value))?;
				if let Ok(entry) = bson::from_reader::<_, KvEntries>(reader) {
					if let KvEntries::SET(key_, value) = entry {
						if key == key_ { return Ok(Some(value)) }
					}
				}
				// Invalid index happened!
				// The database file has been modified externally.
				// Should not be here
				Err(KvError::InvalidIndex)
			},
			None => Ok(None)
		}
	}
	
	/// Remove a given key `key`
	fn remove(&mut self, key: String) -> Result<()> {
		if self.get(key.clone())?.is_none() { return Err(KvError::KeyNotExist(key)) }
		self.writeback(KvEntries::DELETE(key))?;
		Ok(())
	}
	
	/// Create or open KvStore instance
	fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		let mut db_path = path.into();
		let mut index_path = db_path.clone();
		if db_path.is_dir() {
			db_path = db_path.join("kvs.db");
			index_path = index_path.join("kvs.dir");
		} else {
			index_path = index_path.with_extension("dir");
		}
		
		let mut file_db_handle = OpenOptions::new().read(true).write(true).create(true).open(db_path)?;
		let mut file_index_handle = OpenOptions::new().read(true).write(true).create(true).open(index_path)?;
		
		// Check the present of the database header
		let mut header = if file_db_handle.metadata()?.len() != 0 {
			let mut reader = BufReader::new(&mut file_db_handle);
			match bson::from_reader::<_, KvHeader>(&mut reader) {
				Ok(header_entry) => header_entry,
				Err(_) => { return Err(KvError::InvalidDatabaseFormat) }
			}
			// Blank database file
		} else {
			KvHeader {
				build_number: KV_BUILD_NUMBER,
				last_open: 0,
				next_compaction_size: KV_MIN_COMPACTION_THRESHOLD
			}
		};
		
		// Check version
		if header.build_number != KV_BUILD_NUMBER {
			return Err(KvError::IncompatibleDatabaseVersion(header.build_number, KV_BUILD_NUMBER))
		}
		
		// Update last_open timestamp
		header.last_open = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
		file_db_handle.seek(SeekFrom::Start(0))?;
		file_db_handle.write_all(bson::to_vec(&header)?.as_slice())?;
		
		// Reindex if index file not exist or has zero length
		let mut index = if file_index_handle.metadata()?.len() == 0 {
			KvStore::reindex(&mut file_db_handle)?
		} else { HashMap::new() };
		
		let mut reader = BufReader::new(&mut file_index_handle);
		// Build index from existing index file
		while let Ok(entry) = bson::from_reader::<_, KvIndexEntries>(&mut reader) {
			index.insert(entry.key, entry.offset);
		}
		
		Ok(KvStore {
			header,
			file_db_handle,
			file_index_handle,
			index,
			is_compaction: false,
			modified: false
		})
	}
}

impl KvStore {
	/// Re-index the database file
	fn reindex(file_db_handle: &mut File) -> Result<HashMap<String, u64>> {
		let mut offset = 0;
		let mut index = HashMap::new();
		let mut reader = BufReader::new(file_db_handle);
		reader.seek(SeekFrom::Start(0))?;
		// Skip header
		bson::from_reader::<_, KvHeader>(&mut reader)?;
		// Iterate the all entries in the database file
		while let Ok(entry) = bson::from_reader::<_, KvEntries>(&mut reader) {
			match entry {
				KvEntries::SET(key, _) => { index.insert(key, offset); },
				KvEntries::DELETE(key) => { index.remove(&key); }
			}
			// Store the start offset of next entry
			offset = reader.seek(SeekFrom::Current(0))?;
		}
		Ok(index)
	}
	
	/// Do compaction if the database file size reaches threshold
	fn compaction(&mut self) -> Result<()> {
		// Incident the ongoing compaction
		// Avoid recursive calling of compaction() when the database file does not contain enough redundant entries
		self.is_compaction = true;
		let mut entries = HashMap::new();
		for (key, _) in self.index.iter() {
			if let Some(value) = self.get(key.clone())? {
				entries.insert(key.clone(), value);
			}
		}
		self.file_db_handle.set_len(0)?;
		// Build header
		self.update_header()?;
		self.index.clear();
		for (key, value) in entries {
			self.writeback(KvEntries::SET(key, value))?;
		}
		// Estimate next compaction size: Double the current size
		self.header.next_compaction_size = max(self.file_db_handle.metadata()?.len() * 2, KV_MIN_COMPACTION_THRESHOLD);
		self.update_header()?;
		// Incident the end of compaction
		self.is_compaction = false;
		Ok(())
	}
	
	fn writeback(&mut self, entry: KvEntries) -> Result<()> {
		let serialized = bson::to_vec(&entry)?;
		let offset = self.file_db_handle.seek(SeekFrom::End(0))?;
		self.file_db_handle.write_all(serialized.as_slice())?;
		match entry {
			KvEntries::SET(key, _) => { self.index.insert(key, offset); },
			KvEntries::DELETE(key) => { self.index.remove(&key); }
		}
		self.modified = true;
		// Avoid recursive calling of compaction()
		if !self.is_compaction && self.file_db_handle.metadata()?.len() > self.header.next_compaction_size {
			self.compaction()?;
		}
		Ok(())
	}
	
	fn write_index(&mut self) -> Result<()> {
		// Skip rewriting index if the database has not been modified
		if !self.modified { return Ok(()) }
		self.file_index_handle.set_len(0)?;
		let mut writer = BufWriter::new(&mut self.file_index_handle);
		// Truncate file won't change the cursor
		writer.seek(SeekFrom::Start(0))?;
		for (key, offset) in self.index.iter() {
			let entry = KvIndexEntries {
				key: key.clone(),
				offset: *offset
			};
			let serialized = bson::to_vec(&entry)?;
			writer.write_all(serialized.as_slice())?;
		}
		Ok(())
	}
	
	/// Update database file header
	fn update_header(&mut self) -> Result<()> {
		self.file_db_handle.seek(SeekFrom::Start(0))?;
		let serialized = bson::to_vec(&self.header)?;
		self.file_db_handle.write_all(serialized.as_slice())?;
		Ok(())
	}
}

impl Drop for KvStore {
	fn drop(&mut self) {
		self.write_index().unwrap();
	}
}

impl KvsServer {
	pub fn open(engine_type: &str, path: impl Into<PathBuf>) -> Result<KvsServer> {
		unimplemented!()
	}
	
	pub fn start(&mut self) -> Result<()> {
		unimplemented!()
	}
	
	pub fn terminate(&mut self) -> Result<()> {
		unimplemented!()
	}
}

#[test]
fn test_kv() -> Result<()> {
	let mut store = KvStore::open("kvs.db")?;
	store.set("user.root.password".to_string(), "archlinuxisthebest".to_string())?;
	store.set("user.root.name".to_string(), "系統管理員".to_string())?;
	store.set("user.root.balance".to_string(), "50000".to_string())?;
	store.set("user.root.money".to_string(), "100000".to_string())?;
	store.remove("user.root.money".to_string())?;
	store.set("user.root.balance".to_string(), "0".to_string())?;
	store.set("user.root.password".to_string(), "archlinuxforever".to_string())?;
	store.set("user.root.name".to_string(), "神".to_string())?;
	assert_eq!(store.get("user.root.name".to_string())?, Some("神".to_string()));
	assert_eq!(store.get("user.root.password".to_string())?, Some("archlinuxforever".to_string()));
	assert_eq!(store.get("user.root.balance".to_string())?, Some("0".to_string()));
	Ok(())
}

#[test]
fn test_kv_compact() -> Result<()> {
	let mut store = KvStore::open("kvs.db")?;
	store.compaction()?;
	Ok(())
}
