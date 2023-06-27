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

mod store;
mod engine;
mod server;
mod client;
mod sled;
mod errors;

// Public export symbol
pub mod util;
pub use self::store::KvStore;
pub use self::engine::KvsEngine;
pub use self::server::KvsServer;
pub use self::client::KvsClient;
pub use self::errors::{KvsError, Result};
pub use self::sled::SledKvsEngine;

// Internal use
use self::server::{KvsCmdRequest, KvsServerReply, KvsServerReplyStatus};
