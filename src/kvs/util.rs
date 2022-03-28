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

use super::Result;
use std::thread;

pub trait ThreadPool {
	/// Creates a new thread pool, immediately spawning the specified number of threads
	fn new(thread: u32) -> Result<Self> where Self: Sized;
	/// Spawn a function into the thread pool
	fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
	fn new(thread: u32) -> Result<Self> where Self: Sized {
		Ok(NaiveThreadPool)
	}
	
	fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
		thread::spawn(job);
	}
}

pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
	fn new(thread: u32) -> Result<Self> where Self: Sized {
		todo!()
	}
	
	fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
		todo!()
	}
}

pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
	fn new(thread: u32) -> Result<Self> where Self: Sized {
		todo!()
	}
	
	fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
		todo!()
	}
}
