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

use failure::{Error, format_err};
use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use serde::{Deserialize, Serialize};
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct KvStore {
	file_handle: File,
	last_pos: u64,
	entries: HashMap<String, String>
}

#[derive(Serialize, Deserialize, Debug)]
enum KvEntries {
	SET(String, String),
	DELETE(String)
}

impl KvStore {
	/// Set the value of a string key to a string
	pub fn set(&mut self, key: String, value: String) -> Result<()> {
		let entry = KvEntries::SET(key, value);
		let serialized = bson::to_vec(&entry)?;
		self.file_handle.seek(SeekFrom::End(0))?;
		self.file_handle.write_all(serialized.as_slice())?;
		Ok(())
	}
	
	/// Get the string value of a given string key
	pub fn get(&mut self, key: String) -> Result<Option<String>> {
		self.update()?;
		match self.entries.get(&key) {
			Some(value) => Ok(Some(value.clone())),
			None => Ok(None)
		}
	}
	
	/// Remove a given key `key`
	pub fn remove(&mut self, key: String) -> Result<()> {
		self.update()?;
		if !self.entries.contains_key(&key) { return Err(format_err!("Key not found")) }
		let entry = KvEntries::DELETE(key);
		let serialized = bson::to_vec(&entry)?;
		self.file_handle.seek(SeekFrom::End(0))?;
		self.file_handle.write_all(serialized.as_slice())?;
		Ok(())
	}
	
	/// Create new KvStore from file
	pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		let mut path = path.into();
		if path.is_dir() { path = path.join("kvs.db") }
		let file_handle = OpenOptions::new().read(true).append(true).create(true).open(path)?;
		Ok(KvStore {
			file_handle,
			last_pos: 0,
			entries: HashMap::new()
		})
	}
	
	/// Update in-memory cache to latest change of the database file
	fn update(&mut self) -> Result<()> {
		let mut reader = BufReader::new(self.file_handle.try_clone()?);
		reader.seek(SeekFrom::Start(self.last_pos))?;
		while let Ok(entry) = bson::from_reader::<_, KvEntries>(&mut reader) {
			match entry {
				KvEntries::SET(key, value) => { self.entries.insert(key, value); },
				KvEntries::DELETE(key) => { self.entries.remove(&key); }
			}
		}
		self.last_pos = reader.seek(SeekFrom::Current(0))?;
		Ok(())
	}
	
	/// Do compaction if the database file size reaches threshold
	fn compaction(&mut self) -> Result<()> {
		udighkidjlrhgnilivevudfigttucghl
		todo!()
	}
}

#[test]
fn test_kv() -> Result<()> {
	let mut store = KvStore::open("/tmp/kvs.db")?;
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
