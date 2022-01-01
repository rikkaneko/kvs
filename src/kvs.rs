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
use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use serde::{Deserialize, Serialize};
use thiserror::Error;
pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug)]
pub struct KvStore {
	file_db_handle: File,
	file_index_handle: File,
	index: HashMap<String, u64>,
	modified: bool
}

#[derive(Error, Debug)]
pub enum KvError {
	#[error("I/O error")]
	IOError(#[from] std::io::Error),
	#[error(r#"Key "{0}" does not exist"#)]
	KeyNotExist(String),
	#[error("Invalid index detected. The database file has been modified externally")]
	InvalidIndex,
	#[error("Serialization Error")]
	SerializationError(#[from] bson::ser::Error)
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

impl KvStore {
	const COMPACTION_THERHOLD: u64 = 128*1024; // 128KiB
	/// Set the value of a string key to a string
	pub fn set(&mut self, key: String, value: String) -> Result<()> {
		self.writeback(KvEntries::SET(key, value))?;
		Ok(())
	}
	
	/// Get the string value of a given string key
	pub fn get(&self, key: String) -> Result<Option<String>> {
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
	pub fn remove(&mut self, key: String) -> Result<()> {
		if self.get(key.clone())?.is_none() { return Err(KvError::KeyNotExist(key)) }
		self.writeback(KvEntries::DELETE(key))?;
		Ok(())
	}
	
	/// Create or open KvStore instance
	pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		let mut db_path = path.into();
		let mut index_path = db_path.clone();
		if db_path.is_dir() {
			db_path = db_path.join("kvs.db");
			index_path = index_path.join("kvs.dir");
		} else {
			index_path = index_path.with_extension("dir");
		}
		
		let mut file_db_handle = OpenOptions::new().read(true).write(true).create(true).open(db_path)?;
		let file_index_handle = OpenOptions::new().read(true).write(true).create(true).open(index_path)?;
		// Reindex if index file not exist or has zero length
		let mut index = if file_index_handle.metadata()?.len() == 0 {
			KvStore::reindex(&mut file_db_handle)?
		} else { HashMap::new() };
		
		let mut reader = BufReader::new(file_index_handle.try_clone()?);
		// Build index from existing index file
		while let Ok(entry) = bson::from_reader::<_, KvIndexEntries>(&mut reader) {
			index.insert(entry.key, entry.offset);
		}
		
		Ok(KvStore {
			file_db_handle,
			file_index_handle,
			index,
			modified: false
		})
	}
	
	/// Re-index the database file
	fn reindex(file_db_handle: &mut File) -> Result<HashMap<String, u64>> {
		let mut offset = 0;
		let mut index = HashMap::new();
		let mut reader = BufReader::new(file_db_handle);
		reader.seek(SeekFrom::Start(0))?;
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
		let mut entries = HashMap::new();
		for (key, _) in self.index.iter() {
			if let Some(value) = self.get(key.clone())? {
				entries.insert(key.clone(), value);
			}
		}
		self.file_db_handle.set_len(0)?;
		self.index.clear();
		for (key, value) in entries {
			self.writeback(KvEntries::SET(key, value))?;
		}
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
		if self.file_db_handle.metadata()?.len() > KvStore::COMPACTION_THERHOLD {
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
}

impl Drop for KvStore {
	fn drop(&mut self) {
		self.write_index().unwrap();
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
