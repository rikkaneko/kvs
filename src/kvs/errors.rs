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

use thiserror::Error;
pub type Result<T> = std::result::Result<T, KvsError>;

// Specified all error kind may occurred in kvs library
#[derive(Error, Debug)]
pub enum KvsError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(r#"Key "{0}" does not exist"#)]
    KeyNotExist(String),
    #[error("Invalid data entry")]
    InvalidDataEntry,
    #[error(transparent)]
    SerializationError(#[from] bson::ser::Error),
    #[error(transparent)]
    DeserializationError(#[from] bson::de::Error),
    #[error("Unsupported engine type")]
    UnsupportedEngine,
    #[error("Invalid database file format")]
    InvalidDatabaseFormat,
    #[error("Found incompatible database version {0}, current version {1}")]
    IncompatibleDatabaseVersion(u64, u64),
    #[error(transparent)]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Unknown protocol")]
    UnknownProtocol,
    #[error("Server internal error")]
    ServerError,
    #[error(transparent)]
    InvalidAddress(#[from] std::net::AddrParseError),
    #[error(transparent)]
    SledError(#[from] sled::Error)
}
