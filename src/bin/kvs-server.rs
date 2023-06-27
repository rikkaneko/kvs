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
extern crate slog;
#[macro_use]
extern crate clap;
use std::{env, thread};
use std::fs::OpenOptions;
use std::path::PathBuf;
use clap::App;
#[cfg(target_os = "linux")]
use signal_hook::{consts::{SIGINT, SIGTERM}, iterator::Signals};
use kvs::kvs::{Result, KvsServer, KvsClient};
use slog::{Duplicate, Drain, info, Logger};
use slog_term::{FullFormat, PlainDecorator, TermDecorator};
use slog_async::{Async};

fn main() -> Result<()> {
    let yaml = load_yaml!("kvs_server.yaml");
    let args = App::from_yaml(yaml)
        .version(env!("CARGO_PKG_VERSION"))
        .get_matches();
    
    let addr = args.value_of("addr").unwrap();
    let engine = args.value_of("engine").unwrap();
    let path = PathBuf::from(args.value_of("basedir").unwrap()).canonicalize()?;
    
    let logfile = OpenOptions::new().create(true).write(true).truncate(true).open(path.join("stderr"))?;
    let term_drain = FullFormat::new(TermDecorator::new().build()).build();
    let file_drain = FullFormat::new(PlainDecorator::new(logfile)).build();
    let drains = Duplicate::new(term_drain, file_drain).fuse();
    let (drain, _guard) = Async::new(drains).build_with_guard();
    let logger = Logger::root(drain.fuse(), o!());
    
    
    // Signal handler
    // Currently only support Linux for signal handling
    // TODO Signal handling for Windows platform
    #[cfg(target_os = "linux")] {
        let _addr = addr.to_owned();
        let _logger = logger.clone();
        let mut signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
        thread::spawn(move || -> Result<()> {
            if signals.forever().next().is_some() {
                // Send the termination signal
                KvsClient::open(&_addr)?.send_terminate_signal()?;
                warn!(_logger, "Terminated by signal");
            }
            Ok(())
        });
    }
    
    // Check previously used database engine
    // kvs: kvs.db and kvs.dir
    // sled: db, config and blob directory
    if (path.join("kvs.db").exists() && engine != "kvs")
        || (path.join("db").exists() && engine != "sled") {
        error!(logger, "Conflicted engine detected";
			"path" => path.to_str().unwrap(), "engine" => engine);
        info!(logger, "Consider change the working directory with --base-dir options.");
        quit::with_code(255);
        // It is the most weird thing I known in Rust that std::process::exit() does not call destructor ¯\_ಠ_ಠ_/¯
        // exit(255);
    }
    
    info!(logger, "kvs-server";
		"addr" => addr, "path" => path.to_str().unwrap(), "engine" => engine, "version" => env!("CARGO_PKG_VERSION"));
    
    KvsServer::open(engine, path)?.start(addr)?;
    info!(logger, "Server shutdown gratefully");
    Ok(())
}
