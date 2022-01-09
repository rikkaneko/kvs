/*
 * This file is part of kvs.
 * Copyright (c) 2022 Joe Ma <rikkaneko23@gmail.com>
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

use std::cmp::max;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use super::{KvsEngine, KvsError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct KvStore {
	header: KvHeader,
	file_db_handle: File,
	file_index_handle: File,
	index: HashMap<String, u64>,
	is_compaction: bool,
	modified: bool
}

// In-disk data format for KvStore database file entries
#[derive(Serialize, Deserialize, Debug)]
enum KvsEntries {
	SET(String, String),
	DELETE(String)
}

// In-disk data format for KvStore index file entries
#[derive(Serialize, Deserialize, Debug)]
struct KvsIndexEntries {
	key: String,
	offset: u64
}

// In-disk data format for KvStore database file header
#[derive(Serialize, Deserialize, Debug)]
struct KvHeader {
	build_number: u64,
	last_open: u64,
	next_compaction_size: u64,
	// in byte
	// 0x1: is_last_graceful_exit
	flags: u64
}

impl KvsEngine for KvStore {
	/// Set the value of a string key to a string
	fn set(&mut self, key: String, value: String) -> Result<()> {
		self.writeback(KvsEntries::SET(key, value))?;
		Ok(())
	}
	
	/// Get the string value of a given string key
	fn get(&self, key: String) -> Result<Option<String>> {
		match self.index.get(&key) {
			Some(value) => {
				let mut reader = BufReader::new(self.file_db_handle.try_clone()?);
				reader.seek(SeekFrom::Start(*value))?;
				if let Ok(KvsEntries::SET(key_, value)) = bson::from_reader::<_, KvsEntries>(reader) {
					if key == key_ { return Ok(Some(value)) }
				}
				Err(KvsError::InvalidDataEntry)
			},
			None => Ok(None)
		}
	}
	
	/// Remove a given key `key`
	fn remove(&mut self, key: String) -> Result<()> {
		if self.get(key.clone())?.is_none() { return Err(KvsError::KeyNotExist(key)) }
		self.writeback(KvsEntries::DELETE(key))?;
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
		let file_index_handle = OpenOptions::new().read(true).write(true).create(true).open(index_path)?;
		
		// Check the present of the database header
		let header = if file_db_handle.metadata()?.len() != 0 {
			let mut reader = BufReader::new(&mut file_db_handle);
			match bson::from_reader::<_, KvHeader>(&mut reader) {
				Ok(header_entry) => header_entry,
				Err(_) => { return Err(KvsError::InvalidDatabaseFormat) }
			}
			// Blank database file
		} else {
			KvHeader {
				build_number: KvStore::BUILD_NUMBER,
				last_open: 0,
				next_compaction_size: KvStore::MIN_COMPACTION_THRESHOLD,
				flags: 0
			}
		};
		
		let mut store = KvStore {
			header,
			file_db_handle,
			file_index_handle,
			index: HashMap::new(),
			is_compaction: false,
			modified: false
		};
		
		// Reindex if index file has zero length (just created) or is_last_graceful_exit bit is 1
		if store.file_index_handle.metadata()?.len() != 0 && store.header.flags & 0x1 == 0 {
			let mut reader = BufReader::new(&mut store.file_index_handle);
			// Build index from existing index file
			while let Ok(entry) = bson::from_reader::<_, KvsIndexEntries>(&mut reader) {
				store.index.insert(entry.key, entry.offset);
			}
		} else {
			store.modified = true;
			store.reindex()?;
		}
		
		// Update last_open timestamp
		store.header.last_open = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
		store.header.flags = 0x1;
		store.update_header()?;
		
		Ok(store)
	}
}

impl KvStore {
	const BUILD_NUMBER: u64 = 1001;
	const MIN_COMPACTION_THRESHOLD: u64 = 8192;
	/// Re-index the database file
	fn reindex(&mut self) -> Result<()> {
		if self.file_db_handle.metadata()?.len() == 0 { return Ok(()) }
		let mut reader = BufReader::new(&mut self.file_db_handle);
		reader.seek(SeekFrom::Start(0))?;
		self.index.clear();
		// Skip header
		bson::from_reader::<_, KvHeader>(&mut reader)?;
		let mut offset = reader.seek(SeekFrom::Current(0))?;
		// Iterate the all entries in the database file
		while let Ok(entry) = bson::from_reader::<_, KvsEntries>(&mut reader) {
			match entry {
				KvsEntries::SET(key, _) => { self.index.insert(key, offset); },
				KvsEntries::DELETE(key) => { self.index.remove(&key); }
			}
			// Store the start offset of next entry
			offset = reader.seek(SeekFrom::Current(0))?;
		}
		// Write back the index file after re-index
		self.write_index()?;
		Ok(())
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
			self.writeback(KvsEntries::SET(key, value))?;
		}
		// Estimate next compaction size: Double the current size
		self.header.next_compaction_size = max(self.file_db_handle.metadata()?.len() * 2, KvStore::MIN_COMPACTION_THRESHOLD);
		self.update_header()?;
		// Incident the end of compaction
		self.is_compaction = false;
		Ok(())
	}
	
	fn writeback(&mut self, entry: KvsEntries) -> Result<()> {
		let offset = self.file_db_handle.seek(SeekFrom::End(0))?;
		self.file_db_handle.write_all(bson::to_vec(&entry)?.as_slice())?;
		match entry {
			KvsEntries::SET(key, _) => { self.index.insert(key, offset); },
			KvsEntries::DELETE(key) => { self.index.remove(&key); }
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
			let entry = KvsIndexEntries {
				key: key.clone(),
				offset: *offset
			};
			writer.write_all(bson::to_vec(&entry)?.as_slice())?;
		}
		Ok(())
	}
	
	/// Update database file header
	fn update_header(&mut self) -> Result<()> {
		self.file_db_handle.seek(SeekFrom::Start(0))?;
		self.file_db_handle.write_all(bson::to_vec(&self.header)?.as_slice())?;
		Ok(())
	}
}

impl Drop for KvStore {
	fn drop(&mut self) {
		self.write_index().unwrap();
		self.header.flags = 0;
		self.update_header().unwrap();
	}
}

#[test]
fn test_kv() -> Result<()> {
	let mut store = KvStore::open("/tmp").expect("Cannot open the database file.");
	store.set("user.root.password".to_owned(), "archlinuxisthebest".to_owned())?;
	store.set("user.root.name".to_owned(), "系統管理員".to_owned())?;
	store.set("user.root.balance".to_owned(), "50000".to_owned())?;
	store.set("user.root.money".to_owned(), "100000".to_owned())?;
	store.remove("user.root.money".to_owned())?;
	store.set("user.root.balance".to_owned(), "0".to_owned())?;
	store.set("user.root.password".to_owned(), "archlinuxforever".to_owned())?;
	store.set("user.root.name".to_owned(), "神".to_owned())?;
	assert_eq!(store.get("user.root.name".to_owned())?, Some("神".to_owned()));
	assert_eq!(store.get("user.root.password".to_owned())?, Some("archlinuxforever".to_owned()));
	assert_eq!(store.get("user.root.balance".to_owned())?, Some("0".to_owned()));
	Ok(())
}
