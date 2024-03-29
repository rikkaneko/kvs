/*
 * This file is part of kvs.
 * Copyright (c) 2022-2023 Joe Ma <rikkaneko23@gmail.com>
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use super::{KvsEngine, KvsError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct KvStoreInt {
    header: KvHeader,
    index: HashMap<String, u64>,
    modified: bool, // Trigger index update when drop
    db_path: PathBuf,
    index_path: PathBuf
}

#[derive(Clone, Debug)]
pub struct KvStore {
    store: Arc<RwLock<KvStoreInt>>,
    compaction_guard: Arc<RwLock<()>>,
    db_path: Box<PathBuf>,
    db_offset: Arc<AtomicU64> // Next writable database file offset
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
        let mut db_reader = BufReader::new(OpenOptions::new().read(true).write(true).create(true).open(&db_path)?);
        let mut db_writer = BufWriter::new(OpenOptions::new().read(true).write(true).create(true).open(&db_path)?);
        
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
        
        let store = KvStoreInt {
            header,
            index,
            modified: false,
            db_path: db_path.clone(),
            index_path
        };
        
        Ok(KvStore {
            store: Arc::new(RwLock::new(store)),
            compaction_guard: Arc::new(RwLock::new(())),
            db_path: Box::new(db_path),
            db_offset: Arc::new(AtomicU64::new(db_reader.seek(SeekFrom::End(0))?))
        })
    }
}

impl KvStore {
    const BUILD_NUMBER: u64 = 1200;
    const MIN_COMPACTION_THRESHOLD: u64 = 32768;
    
    fn check_compaction(&self) -> Result<bool> {
        // Block any read/write operation until compaction completed
        // Also, wait for other read/write operation to complete
        if self.db_offset.load(Ordering::Relaxed) >= self.store.read().unwrap().header.next_compaction_size {
            self.compaction()?;
            Ok(true)
        } else { Ok(false) }
    }
    
    /// Do compaction if the database file size reaches threshold
    fn compaction(&self) -> Result<()> {
        let _lock = self.compaction_guard.write().unwrap();
        let mut store = self.store.write().unwrap();
        // Avoid negative indication
        if self.db_offset.load(Ordering::Relaxed) < store.header.next_compaction_size {
            return Ok(())
        }
        
        let mut entries = HashMap::new();
        let mut reader: BufReader<File> = BufReader::new(OpenOptions::new().read(true).open(&store.db_path)?);
        
        for (key, offset) in store.index.iter() {
            reader.seek(SeekFrom::Start(*offset))?;
            if let Ok(KvsEntries::SET(key_, value)) = bson::from_reader::<_, KvsEntries>(&mut reader) {
                if key_ == *key { entries.insert(key_, value); }
                else { return Err(KvsError::InvalidDataEntry) }
            }
        }
        
        drop(reader);
        // Clear file content
        let mut writer: BufWriter<File> = BufWriter::new(OpenOptions::new().write(true).truncate(true).open(&*store.db_path)?);
        
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
        
        // Estimate next compaction size: Double the current size
        // Update header
        store.header.next_compaction_size = max(self.db_offset.load(Ordering::Relaxed) * 2, KvStore::MIN_COMPACTION_THRESHOLD);
        KvStore::write_header(&store.header, &mut writer)?;
        
        // Reset db_offset
        self.db_offset.store(writer.seek(SeekFrom::End(0))?, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// Insert entry to the database file
    fn writeback(&self, entry: KvsEntries) -> Result<()> {
        let mut handle = OpenOptions::new().write(true).open(&*self.db_path)?;
        let ent_bytes = bson::to_vec(&entry)?;
        let _lock = self.compaction_guard.read().unwrap(); // Block compaction until completed
        let offset = self.db_offset.fetch_add(ent_bytes.len() as u64, Ordering::Relaxed);
        // Write the entry with the specified offset
        handle.seek(SeekFrom::Start(offset))?;
        handle.write_all(ent_bytes.as_slice())?;
        
        let mut store = self.store.write().unwrap();
        match entry {
            KvsEntries::SET(key, _) => 'blk1: {
                if let Some(offset_) = store.index.get(&key) {
                    if *offset_ > offset { break 'blk1; }
                }
                store.index.insert(key, offset);
            },
            KvsEntries::DELETE(key) => 'blk2: {
                if let Some(offset_) = store.index.get(&key) {
                    if *offset_ > offset { break 'blk2; }
                }
                store.index.remove(&key);
            }
        }
        store.modified = true;
        Ok(())
    }
    
    /// Fetch entry with the given `key`
    fn fetch(&self, key: String) -> Result<Option<String>> {
        let _lock = self.compaction_guard.read().unwrap(); // Block compaction until completed
        let result = self.store.read().unwrap().index.get(&key).cloned();
        if let Some(offset) = result {
            let mut handle = OpenOptions::new().read(true).open(&*self.db_path)?;
            handle.seek(SeekFrom::Start(offset))?;
            if let Ok(KvsEntries::SET(key_, value)) = bson::from_reader::<_, KvsEntries>(handle.by_ref()) {
                if key == key_ {
                    return Ok(Some(value))
                }
            }
            Err(KvsError::InvalidDataEntry)
        } else { Ok(None) }
    }
    
    /// Rewrite the current index file
    fn write_index(index: &HashMap<String, u64>, db_path: &PathBuf) -> Result<()> {
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

impl Drop for KvStoreInt {
    fn drop(&mut self) {
        // Rewrite index if modified
        if self.modified {
            // Rewrite index file
            KvStore::write_index(&self.index, &self.index_path).unwrap();
        }
        // Set last_graceful_exit bit
        self.header.flags = 0x0;
        KvStore::write_header(&self.header, OpenOptions::new().write(true).open(&*self.db_path).unwrap()).unwrap();
    }
}
