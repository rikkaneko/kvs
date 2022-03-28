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
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use super::{KvsEngine, KvsError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct KvStoreInt {
	header: KvHeader,
	index: HashMap<String, u64>,
	modified: bool, // Trigger index update when drop
	db_offset: u64 // Current database file end
}

#[derive(Clone, Debug)]
pub struct KvStore {
	store: Arc<RwLock<KvStoreInt>>,
	compaction_guard: Arc<RwLock<()>>,
	db_path: Arc<PathBuf>,
	index_path: Arc<PathBuf>
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
	fn set(&self, key: String, value: String) -> Result<()> {
		self.writeback(KvsEntries::SET(key, value))?;
		// Check if compaction condition meet
		self.check_compaction()?;
		Ok(())
	}
	
	/// Get the string value of a given string key
	fn get(&self, key: String) -> Result<Option<String>> {
		self.fetch(key)
	}
	
	/// Remove a given key `key`
	fn remove(&self, key: String) -> Result<()> {
		if !self.store.read().unwrap().index.contains_key(&key) { return Err(KvsError::KeyNotExist(key)) }
		self.writeback(KvsEntries::DELETE(key))?;
		self.check_compaction()?;
		Ok(())
	}
	
	/// Create or open KvStore instance
	fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		// Resolve actual database and index path
		let mut db_path = path.into();
		let mut index_path = db_path.clone();
		if db_path.is_dir() {
			db_path = db_path.join("kvs.db");
			index_path = index_path.join("kvs.dir");
		} else {
			index_path = index_path.with_extension("dir");
		}
		
		// Open and create the database file if not exist
		let mut db_handle = OpenOptions::new().read(true).write(true).create(true).open(&db_path)?;
		let mut db_reader = BufReader::new(db_handle.by_ref());
		let mut db_writer = BufWriter::new(db_handle.by_ref());
		
		// Check the present of the database header
		let mut header = if db_path.metadata()?.len() != 0 {
			match bson::from_reader::<_, KvHeader>(&mut db_reader) {
				Ok(header_entry) => header_entry,
				Err(_) => { return Err(KvsError::InvalidDatabaseFormat) }
			}
		// Blank database file
		} else {
			KvHeader {
				build_number: KvStore::BUILD_NUMBER,
				last_open: 0,
				next_compaction_size: KvStore::MIN_COMPACTION_THRESHOLD,
				flags: 0x1
			}
		};
		
		header.last_open = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
		header.flags = 0x1;
		// Update header
		KvStore::write_header(&header, &mut db_writer)?;
		
		let mut index = HashMap::new();
		// Build index from index file
		// Use existing index only if index file has non zero length and is_last_graceful_exit bit is clear
		if index_path.exists() && index_path.metadata()?.len() != 0 && header.flags & 0x1 == 0 {
			let mut reader = BufReader::new(OpenOptions::new().read(true).open(&index_path)?);
			while let Ok(entry) = bson::from_reader::<_, KvsIndexEntries>(&mut reader) {
				index.insert(entry.key, entry.offset);
			}
		} else {
			// Reindex the database
			let mut offset = db_reader.seek(SeekFrom::Current(0))?;
			while let Ok(entry) = bson::from_reader::<_, KvsEntries>(&mut db_reader) {
				match entry {
					KvsEntries::SET(key, _) => { index.insert(key, offset); },
					KvsEntries::DELETE(key) => { index.remove(&key); }
				}
				// Store the start offset of next entry
				offset = db_reader.seek(SeekFrom::Current(0))?;
			}
			// Rewrite index file
			KvStore::write_index(&index, &index_path)?;
		}
		
		let mut store = KvStoreInt {
			header,
			index,
			modified: false,
			db_offset: db_reader.seek(SeekFrom::End(0))?
		};
		
		Ok(KvStore {
			store: Arc::new(RwLock::new(store)),
			compaction_guard: Arc::new(RwLock::new(())),
			db_path: Arc::new(db_path),
			index_path: Arc::new(index_path)
		})
	}
}

impl KvStore {
	const BUILD_NUMBER: u64 = 1200;
	const MIN_COMPACTION_THRESHOLD: u64 = 8192;
	
	fn check_compaction(&self) -> Result<bool> {
		// Block any read/write operation until compaction completed
		// Also, wait for other read/write operation to complete
		let store = self.store.read().unwrap();
		if store.db_offset >= store.header.next_compaction_size {
			drop(store); // Avoid deadlock
			self.compaction()?;
			Ok(true)
		} else { Ok(false) }
	}
	
	/// Do compaction if the database file size reaches threshold
	fn compaction(&self) -> Result<()> {
		let _lock = self.compaction_guard.write().unwrap();
		let store = self.store.write().unwrap();
		// Avoid negative indication
		if store.db_offset < store.header.next_compaction_size {
			return Ok(())
		}
		
		let mut entries = HashMap::new();
		let mut handle = OpenOptions::new().read(true).write(true).open(*self.db_path)?;
		let mut reader: BufReader<File> = BufReader::new(handle.try_clone()?);
		let mut writer: BufWriter<File> = BufWriter::new(handle.try_clone()?)?;
		
		for (key, offset) in store.index.iter() {
			reader.seek(SeekFrom::Start(*offset))?;
			if let Ok(KvsEntries::SET(key_, value)) = bson::from_reader::<_, KvsEntries>(&mut reader) {
				if key == key_ { entries.insert(key_, value); }
				else { return Err(KvsError::InvalidDataEntry) }
			}
		}
		
		// Clear file content
		handle.set_len(0)?;
		
		// Build header
		let mut offset = KvStore::write_header(&store.header, &mut writer)?;
		// Reset old index
		store.index.clear();
		for (key, value) in entries {
			store.index.insert(key.to_owned(), offset);
			let entry = KvsEntries::SET(key, value);
			writer.write_all(bson::to_vec(&entry)?.as_slice())?;
			offset = writer.seek(SeekFrom::Current(0))?;
		}
		
		// Reset db_offset
		store.db_offset = writer.seek(SeekFrom::End(0))?;
		
		// Estimate next compaction size: Double the current size
		// Update header
		store.header.next_compaction_size = max(handle.metadata()?.len() * 2, KvStore::MIN_COMPACTION_THRESHOLD);
		KvStore::write_header(&store.header, &mut writer)?;
		
		Ok(())
	}
	
	/// Insert entry to the database file
	fn writeback(&self, entry: KvsEntries) -> Result<()> {
		let mut handle = OpenOptions::new().write(true).open(*self.db_path)?;
		let ent_bytes = bson::to_vec(&entry)?;
		let _lock = self.compaction_guard.read().unwrap(); // Block compaction until completed
		let offset = {
			// Atomically reserve space for the entry
			let mut store = self.store.write().unwrap();
			let curr = store.db_offset;
			store.db_offset += ent_bytes.len() as u64;
			curr
		};
		// Write the entry with the specified offset
		handle.seek(SeekFrom::Start(offset))?;
		handle.write_all(ent_bytes.as_slice())?;
		
		let store = self.store.write().unwrap();
		match entry {
			KvsEntries::SET(key, _) => 'blk1:{
				if let Some(offset_) = store.index.get(&key)  {
					if offset_ > *offset { break 'blk1; }
				}
				store.index.insert(key.to_owned(), offset);
			},
			KvsEntries::DELETE(key) => 'blk2:{
				if let Some(offset_) = store.index.get(&key) {
					if offset_ > *offset { break 'blk2; }
				}
				store.index.remove(&key);
			}
		}
		store.modified = true;
		// Compaction condition should be checked with compaction guard
		// Avoid recursive calling of compaction()
		// if !self.is_compaction && self.file_db_handle.metadata()?.len() > self.header.next_compaction_size {
		// 	self.compaction()?;
		// }
		Ok(())
	}
	
	/// Fetch entry with the given `key`
	fn fetch(&self, key: String) -> Result<Option<String>> {
		let _lock = self.compaction_guard.read().unwrap(); // Block compaction until completed
		let result = self.store.read().unwrap().index.get(&key).cloned();
		if let Some(offset) = result {
			let mut handle = OpenOptions::new().read(true).open(*self.db_path)?;
			handle.seek(SeekFrom::Start(offset));
			if let Ok(KvsEntries::SET(key_, value)) = bson::from_reader::<_, KvsEntries>(handle.by_ref()) {
				if key == key_ {
					return Ok(Some(value))
				}
			}
			Err(KvsError::InvalidDataEntry)
		} else { Ok(None) }
	}
	
	/// Rewrite the current index file
	fn write_index<W: Write + Seek>(index: &HashMap<String, u64>, db_path: &PathBuf) -> Result<()> {
		let mut handle = OpenOptions::new().write(true).truncate(true).create(true).open(db_path)?;
		let mut writer = BufWriter::new(handle.by_ref());
		for (key, offset) in index.iter() {
			let entry = KvsIndexEntries {
				key: key.clone(),
				offset: *offset
			};
			writer.write_all(bson::to_vec(&entry)?.as_slice())?;
		}
		Ok(())
	}
	
	/// Update database file header
	fn write_header<W: Write + Seek>(header: &KvHeader, mut writer: W) -> Result<u64> {
		let header_byte = bson::to_vec(header)?;
		writer.seek(SeekFrom::Start(0))?;
		writer.write_all(header_byte.as_slice())?;
		// Remark: The current cursor is now just just after the header region
		Ok(writer.seek(SeekFrom::Current(0))?)
	}
}

impl Drop for KvStore {
	fn drop(&mut self) {
		let mut handle = OpenOptions::new().read(true).write(true).open(&self.db_path).unwrap();
		let store = self.store.read().unwrap();
		// Rewrite index if modified
		if store.modified {
			// Rewrite index file
			KvStore::write_index(&store.index, &self.index_path)?;
		}
		// Set last_graceful_exit bit
		self.header.flags = 0x0;
		KvStore::write_header(&store.header, handle.by_ref()).unwrap();
	}
}
