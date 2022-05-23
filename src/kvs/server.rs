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
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use super::{KvsEngine, KvsError, KvStore, Result};
use super::SledKvsEngine;
use serde::{Deserialize, Serialize};

pub struct KvsServer {
    // TODO Alternative way to hold KvsEngine objects
    store: Box<dyn KvsEngine>,
    need_termination: bool
}

// Communication protocol for Client-Server request (in bson)
#[derive(Serialize, Deserialize, Debug)]
pub struct KvsCmdRequest {
    pub(super) cmd: String,
    pub(super) argument: Vec<String>
}

// Communication protocol for Server-Client reply (in bson)
#[derive(Serialize, Deserialize, Debug)]
pub struct KvsServerReply {
    pub(super) result: Option<String>,
    pub(super) status: KvsServerReplyStatus
}

#[derive(Serialize, Deserialize, Debug)]
pub enum KvsServerReplyStatus {
    Success,
    InvalidArguments,
    InvalidCommand,
    KeyNotFound,
    ServerInternalError
}

impl KvsServer {
    /// Open the database file with specified engine
    pub fn open(engine_type: &str, path: impl Into<PathBuf>) -> Result<KvsServer> {
        // Supported database engine: kvs, sled
        let store: Box<dyn KvsEngine> = match engine_type.to_lowercase().as_ref() {
            "kvs" => Box::new(KvStore::open(path)?),
            "sled" => Box::new(SledKvsEngine::open(path)?),
            _ => { return Err(KvsError::UnsupportedEngine) }
        };
        
        Ok(KvsServer {
            store,
            need_termination: false
        })
    }
    
    /// Start server listening on `addr`
    ///
    /// This method would not return util received termination signal or error
    pub fn start(&mut self, addr: impl ToSocketAddrs) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming().flatten() {
            self.handle_stream(stream)?;
            if self.need_termination { break; }
        }
        Ok(())
    }
    
    /// Handle request from client
    /// KvsServer currently support six command: GET, SET, RM, REMOVE, DELETE, KILL
    fn handle_stream(&mut self, mut stream: TcpStream) -> Result<()> {
        let mut buf = [0; 1024];
        let len = stream.read(&mut buf)?;
        if let Ok(request) = bson::from_slice::<KvsCmdRequest>(&buf[..len]) {
            let reply = match request.cmd.as_ref() {
                "GET" => {
                    if request.argument.len() == 1 {
                        match self.store.get(request.argument.get(0).unwrap().to_owned())? {
                            Some(result) => KvsServerReply {
                                result: Some(result),
                                status: KvsServerReplyStatus::Success
                            },
                            
                            None => KvsServerReply {
                                result: None,
                                status: KvsServerReplyStatus::Success
                            }
                        }
                    } else {
                        KvsServerReply {
                            result: Some(format!("`GET` command required 1 argument, provided {}", request.argument.len())),
                            status: KvsServerReplyStatus::InvalidArguments
                        }
                    }
                },
                
                "SET" => {
                    if request.argument.len() == 2 {
                        match self.store.set(request.argument.get(0).unwrap().to_owned(),
                                             request.argument.get(1).unwrap().to_owned()) {
                            Ok(_) => KvsServerReply {
                                result: None,
                                status: KvsServerReplyStatus::Success
                            },
                            
                            Err(KvsError::KeyNotExist(_)) => KvsServerReply {
                                result: None,
                                status: KvsServerReplyStatus::KeyNotFound
                            },
                            
                            _ => KvsServerReply {
                                result: None,
                                status: KvsServerReplyStatus::ServerInternalError
                            }
                        }
                    } else {
                        KvsServerReply {
                            result: Some(format!("`GET` command required 2 argument, provided {}", request.argument.len())),
                            status: KvsServerReplyStatus::InvalidArguments
                        }
                    }
                },
                
                x @ ("RM" | "REMOVE" | "DELETE") => {
                    if request.argument.len() == 1 {
                        match self.store.remove(request.argument.get(0).unwrap().to_owned()) {
                            Ok(_) => KvsServerReply {
                                result: None,
                                status: KvsServerReplyStatus::Success
                            },
                            
                            Err(KvsError::KeyNotExist(_)) => KvsServerReply {
                                result: None,
                                status: KvsServerReplyStatus::KeyNotFound
                            },
                            
                            _ => KvsServerReply {
                                result: None,
                                status: KvsServerReplyStatus::ServerInternalError
                            }
                        }
                    } else {
                        KvsServerReply {
                            result: Some(format!("`{}` command required 1 argument, provided {}",
                                                 x, request.argument.len())),
                            status: KvsServerReplyStatus::InvalidArguments
                        }
                    }
                },
                
                // Termination
                "KILL" => {
                    if request.argument.is_empty() {
                        self.need_termination = true;
                        KvsServerReply {
                            result: None,
                            status: KvsServerReplyStatus::Success
                        }
                    } else {
                        KvsServerReply {
                            result: Some(format!("`KILL` command required 0 argument, provided {}", request.argument.len())),
                            status: KvsServerReplyStatus::InvalidArguments
                        }
                    }
                }
                
                _ => {
                    KvsServerReply {
                        result: None,
                        status: KvsServerReplyStatus::InvalidCommand
                    }
                }
            };
            // Send reply
            stream.write_all(bson::to_vec(&reply)?.as_slice())?;
        }
        Ok(())
    }
}
