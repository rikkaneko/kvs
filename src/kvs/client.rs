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

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use super::{KvsError, KvsCmdRequest, KvsServerReply, KvsServerReplyStatus, Result};

#[derive(Clone)]
pub struct KvsClient {
    addr: SocketAddr
}

impl KvsClient {
    /// Get the string value of a given string key
    pub fn set(&self, key: String, value: String) -> Result<()> {
        let reply = self.send_and_fetch(KvsCmdRequest {
            cmd: "SET".to_owned(),
            argument: vec![key.to_owned(), value]
        })?;
        
        match reply.status {
            KvsServerReplyStatus::Success => Ok(()),
            KvsServerReplyStatus::KeyNotFound => Err(KvsError::KeyNotExist(key)),
            _ => Err(KvsError::ServerError)
        }
    }
    
    /// Get the string value of a given string key
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let reply = self.send_and_fetch(KvsCmdRequest {
            cmd: "GET".to_owned(),
            argument: vec![key]
        })?;
        
        match reply.status {
            KvsServerReplyStatus::Success => Ok(reply.result),
            _ => Err(KvsError::ServerError)
        }
    }
    
    /// Remove a given key `key`
    pub fn remove(&self, key: String) -> Result<()> {
        let reply = self.send_and_fetch(KvsCmdRequest {
            cmd: "REMOVE".to_owned(),
            argument: vec![key.to_owned()]
        })?;
        
        match reply.status {
            KvsServerReplyStatus::Success => Ok(()),
            KvsServerReplyStatus::KeyNotFound => Err(KvsError::KeyNotExist(key)),
            _ => Err(KvsError::ServerError)
        }
    }
    
    /// Establish connection to KvsServer
    pub fn open(addr: &str) -> Result<KvsClient> {
        Ok(KvsClient {
            addr: addr.parse()?
        })
    }
    
    pub fn send_terminate_signal(&mut self) -> Result<()> {
        let reply = self.send_and_fetch(KvsCmdRequest {
            cmd: "KILL".to_owned(),
            argument: Vec::new()
        })?;
        
        match reply.status {
            KvsServerReplyStatus::Success => Ok(()),
            _ => Err(KvsError::ServerError)
        }
    }
    
    fn send_and_fetch(&self, request: KvsCmdRequest) -> Result<KvsServerReply> {
        // Send request
        let mut conn = TcpStream::connect(self.addr)?;
        conn.write_all(bson::to_vec(&request)?.as_slice())?;
        let mut buf = [0; 1024];
        // Wait for server reply
        let len = conn.read(&mut buf)?;
        Ok(bson::from_slice::<KvsServerReply>(&buf[..len])?)
    }
}
