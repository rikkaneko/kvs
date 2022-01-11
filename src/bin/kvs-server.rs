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

#[macro_use]
extern crate clap;
use std::env;
use std::path::PathBuf;
use std::process::exit;
use clap::App;
#[cfg(target_os = "linux")]
use signal_hook::{consts::{SIGINT, SIGTERM}, iterator::Signals};
use kvs::kvs::{Result, KvsServer, KvsClient};

fn main() -> Result<()> {
	#[cfg(target_os = "linux")]
		let mut signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
	let yaml = load_yaml!("kvs_server.yaml");
	let args = App::from_yaml(yaml)
		.version(env!("CARGO_PKG_VERSION"))
		.get_matches();
	
	let addr = args.value_of("addr").unwrap();
	let engine = args.value_of("engine").unwrap();
	let path = args.value_of("basedir").unwrap();
	let _addr = addr.to_owned();
	
	// Signal handler
	// Currently only support Linux for signal handling
	// TODO Signal handling for Windows platform
	#[cfg(target_os = "linux")] {
		use std::thread;
		thread::spawn(move || -> Result<()> {
			for _ in signals.forever() {
				// Send the termination signal
				KvsClient::open(&_addr)?.send_terminate_signal()?;
				println!("Terminated by signal.");
			}
			Ok(())
		});
	}
	
	// Check previously used database engine
	// kvs: kvs.db and kvs.dir
	// sled: db, config and blob directory
	let db_path = PathBuf::from(path);
	if (db_path.join("kvs.db").exists() && engine != "kvs")
		|| (db_path.join("db").exists() && engine != "sled") {
		eprintln!("{} had been used with {} engine already.", env::current_dir()?.to_str().unwrap(), engine);
		println!("Consider change the working directory with --base-dir options.");
		exit(255);
	}
	
	println!("Database engine: {}", engine);
	println!("The server is listening on {}", addr);
	println!("Working directory: {} ({})", path, env::current_dir()?.to_str().unwrap());
	KvsServer::open(engine, path)?.start(addr)?;
	
	Ok(())
}
