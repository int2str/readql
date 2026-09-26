// Copyright (C) 2026 readql contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

//!
//! Buffered chunk writer adapting [`std::io::Write`] to Tokio [`mpsc::Sender`] channels.
//!

use std::io::{self, ErrorKind, Write};

use bytes::Bytes;
use tokio::sync::mpsc;

/// Adapter implementing [`std::io::Write`] that buffers bytes and sends chunks
/// over a Tokio [`mpsc::Sender`] channel for streaming HTTP responses.
pub struct ChunkWriter {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
    buffer: Vec<u8>,
    chunk_size: usize,
}

impl ChunkWriter {
    /// Creates a new `ChunkWriter` with the given channel sender and chunk buffer size.
    pub fn new(sender: mpsc::Sender<Result<Bytes, io::Error>>, chunk_size: usize) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(chunk_size + 4096),
            chunk_size,
        }
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, bytes_slice: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes_slice);
        if self.buffer.len() >= self.chunk_size {
            let chunk = Bytes::from(std::mem::replace(
                &mut self.buffer,
                Vec::with_capacity(self.chunk_size + 4096),
            ));
            if self.sender.blocking_send(Ok(chunk)).is_err() {
                return Err(io::Error::from(ErrorKind::BrokenPipe));
            }
        }
        Ok(bytes_slice.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            let chunk = Bytes::from(std::mem::take(&mut self.buffer));
            if self.sender.blocking_send(Ok(chunk)).is_err() {
                return Err(io::Error::from(ErrorKind::BrokenPipe));
            }
        }
        Ok(())
    }
}
