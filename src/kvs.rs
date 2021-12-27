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
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct KvStore {
	db_path: PathBuf
}

impl KvStore {
	/// Set the value of a string key to a string
	pub fn set(&mut self, key: String, value: String) -> Result<()> {
		Ok(())
	}
	
	/// Get the string value of a given string key
	pub fn get(&self, key: String) -> Result<Option<String>> {
		Ok(None)
	}
	
	/// Remove a given key `key`
	pub fn remove(&mut self, key: String) -> Result<()> {
		Ok(())
	}
	
	pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		Ok(KvStore {
			db_path: path.into()
		})
	}
}
