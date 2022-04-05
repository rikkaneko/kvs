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

use std::path::PathBuf;
use super::{KvsEngine, KvsError, Result};

/// Sled storage engine
#[derive(Clone, Debug)]
pub struct SledKvsEngine {
	db: sled::Db
}

impl KvsEngine for SledKvsEngine {
	fn set(&self, key: String, value: String) -> Result<()> {
		self.db.insert(key.as_bytes(), value.as_bytes())?;
		// Add flush
		self.db.flush()?;
		Ok(())
	}
	
	fn get(&self, key: String) -> Result<Option<String>> {
		Ok(self.db.get(key.as_bytes())?.map(|result| String::from_utf8_lossy(result.as_ref()).to_string()))
	}
	
	fn remove(&self, key: String) -> Result<()> {
		if self.db.remove(key.as_bytes())?.is_some() {
			// Add flush
			self.db.flush()?;
			Ok(())
		} else { Err(KvsError::KeyNotExist(key)) }
	}
	
	fn open(path: impl Into<PathBuf>) -> Result<Self> {
		Ok(SledKvsEngine {
			db: sled::open(path.into())?
		})
	}
}
