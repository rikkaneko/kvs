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

use criterion::{criterion_group, criterion_main, Criterion};
use kvs::kvs::{KvsEngine, KvStore, SledKvsEngine};
use rand::distributions::{Distribution, Uniform, Alphanumeric};
use rand::Rng;
use tempfile::TempDir;

fn gen_random_string(n: usize) -> String {
	rand::thread_rng()
		.sample_iter(&Alphanumeric)
		.take(n)
		.map(char::from)
		.collect()
}

fn kvs_benches(c: &mut Criterion) {
	let mut samples: Vec<(String, String)> = Vec::new();
	let temp_dir_kvs = TempDir::new().expect("unable to create temporary working directory");
	let temp_dir_sled = TempDir::new().expect("unable to create temporary working directory");
	let mut rng = rand::thread_rng();
	let uniform = Uniform::<usize>::from(1..=10000);
	samples.reserve(100);
	
	for _ in 1..=100 {
		samples.push(
			(gen_random_string(uniform.sample(&mut rng)), gen_random_string(uniform.sample(&mut rng))))
	}
	
	// With the kvs engine, write 100 values with random keys of length 1-100000 bytes and random values of length 1-100000 bytes
	c.bench_function("kvs_write", |b| {
		b.iter(|| {
			let mut store = KvStore::open(temp_dir_kvs.path()).expect("Unable to open the database");
			for i in 0..100 {
				let (key, value) = samples.get(i).unwrap();
				store.set(key.to_owned(), value.to_owned()).expect("Unable to write to the database");
			}
		});
	});
	
	// With the sled engine, write 100 values with random keys of length 1-100000 bytes and random values of length 1-100000 bytes
	c.bench_function("sled_write", |b| {
		b.iter(|| {
			let mut store = SledKvsEngine::open(temp_dir_sled.path()).expect("Unable to open the database");
			for i in 0..100 {
				let (key, value) = samples.get(i).unwrap();
				store.set(key.to_owned(), value.to_owned()).expect("Unable to write to the database");
			}
		});
	});
	
	// With the kvs engine, read 1000 values from previously written keys, with keys and values of random length
	c.bench_function("kvs_read", |b| {
		b.iter(|| {
			let store = KvStore::open(temp_dir_kvs.path()).expect("Unable to open the database");
			for _ in 0..10 {
				for i in 0..100 {
					let (key, value) = samples.get(i).unwrap();
					if store.get(key.to_owned())
						.expect("Unable to read from the database").unwrap().ne(value) {
						panic!("Should not be here")
					}
				}
			}
		});
	});
	
	// With the sled engine, read 1000 values from previously written keys, with keys and values of random length
	c.bench_function("sled_read", |b| {
		b.iter(|| {
			let store = SledKvsEngine::open(temp_dir_sled.path()).expect("Unable to open the database");
			for _ in 0..10 {
				for i in 0..100 {
					let (key, value) = samples.get(i).unwrap();
					if store.get(key.to_owned())
							.expect("Unable to read from the database").unwrap().ne(value) {
						panic!("Should not be here")
					}
				}
			}
		});
	});
	
	
}

criterion_group!(benches, kvs_benches);
criterion_main!(benches);
