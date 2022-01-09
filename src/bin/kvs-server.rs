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
use clap::App;
use kvs::kvs::{Result, KvsServer};

fn main() -> Result<()> {
	let yaml = load_yaml!("kvs_server.yaml");
	let args = App::from_yaml(yaml)
		.version(env!("CARGO_PKG_VERSION"))
		.get_matches();
	
	let addr = args.value_of("addr").unwrap();
	let engine = args.value_of("engine").unwrap();
	let path = args.value_of("basedir").unwrap();
	
	println!("Database engine: {}", engine);
	println!("The server is listening on {}", addr);
	KvsServer::open(engine, path)?.start(addr)?;
	
	Ok(())
}
