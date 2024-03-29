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

#[macro_use]
extern crate clap;
use clap::App;
use kvs::kvs::{Result, KvsError, KvsClient};


fn main() -> Result<()> {
    let yaml = load_yaml!("kvs_client.yaml");
    let args = App::from_yaml(yaml)
        .version(env!("CARGO_PKG_VERSION"))
        .get_matches();
    
    let addr = args.value_of("addr").unwrap();
    
    let mut kv = KvsClient::open(addr)?;
    
    match args.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("KEY").unwrap();
            let value = matches.value_of("VALUE").unwrap();
            if let Err(KvsError::KeyNotExist(_)) = kv.set(key.to_string(), value.to_string()) {
                println!("Key not found");
                quit::with_code(255);
                // exit(255);
            }
        },
        
        ("get", Some(matches)) => {
            let key = matches.value_of("KEY").unwrap();
            match kv.get(key.to_string()) {
                Ok(result) => {
                    if let Some(value) = result {
                        println!("{}", value);
                    } else {
                        println!("Key not found");
                    }
                },
                
                Err(err) => {
                    println!("{}", err.to_string());
                    quit::with_code(255);
                    // exit(255);
                }
            }
        },
        
        ("rm", Some(matches)) => {
            let key = matches.value_of("KEY").unwrap();
            if let Err(KvsError::KeyNotExist(_)) = kv.remove(key.to_string()) {
                eprintln!("Key not found");
                quit::with_code(255);
                // exit(255);
            }
        },
        
        ("terminate", _) => {
            if kv.send_terminate_signal().is_err() {
                quit::with_code(255);
                // exit(255);
            }
        }
        
        _ => {
            quit::with_code(255);
            // exit(1);
        }
    }
    
    Ok(())
}
