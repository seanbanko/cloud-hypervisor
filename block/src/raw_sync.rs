// Copyright © 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0 AND BSD-3-Clause

use crate::async_io::{
    AsyncIo, AsyncIoError, AsyncIoResult, DiskFile, DiskFileError, DiskFileResult,
};
use crate::DiskTopology;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::os::unix::io::{AsRawFd, RawFd};
use vmm_sys_util::eventfd::EventFd;

pub struct RawFileDiskSync {
    file: File,
    logical_block_size: Option<u64>,
}

impl RawFileDiskSync {
    pub fn new(file: File, logical_block_size: Option<u64>) -> Self {
        RawFileDiskSync {
            file,
            logical_block_size,
        }
    }
}

impl DiskFile for RawFileDiskSync {
    fn size(&mut self) -> DiskFileResult<u64> {
        self.file
            .seek(SeekFrom::End(0))
            .map_err(DiskFileError::Size)
    }

    fn new_async_io(&self, _ring_depth: u32) -> DiskFileResult<Box<dyn AsyncIo>> {
        Ok(Box::new(RawFileSync::new(
            self.file.as_raw_fd(),
            self.logical_block_size,
        )) as Box<dyn AsyncIo>)
    }

    fn topology(&mut self) -> DiskTopology {
        let topology = if let Ok(topology) = DiskTopology::probe(&self.file) {
            topology
        } else {
            warn!("Unable to get device topology. Using default topology");
            DiskTopology::default()
        };
        if let Some(logical_block_size) = self.logical_block_size {
            DiskTopology {
                logical_block_size,
                ..topology
            }
        } else {
            topology
        }
    }
}

pub struct RawFileSync {
    fd: RawFd,
    eventfd: EventFd,
    completion_list: VecDeque<(u64, i32)>,
    logical_block_size: Option<u64>,
}

impl RawFileSync {
    pub fn new(fd: RawFd, logical_block_size: Option<u64>) -> Self {
        RawFileSync {
            fd,
            eventfd: EventFd::new(libc::EFD_NONBLOCK).expect("Failed creating EventFd for RawFile"),
            completion_list: VecDeque::new(),
            logical_block_size,
        }
    }

    fn round_down(offset: usize, alignment: usize) -> usize {
        (offset / alignment) * alignment
    }

    fn round_up(offset: usize, alignment: usize) -> usize {
        ((offset + alignment - 1) / alignment) * alignment
    }
}

impl AsyncIo for RawFileSync {
    fn notifier(&self) -> &EventFd {
        &self.eventfd
    }

    fn read_vectored(
        &mut self,
        offset: libc::off_t,
        iovecs: &[libc::iovec],
        user_data: u64,
    ) -> AsyncIoResult<()> {
        if let Some(logical_block_size) = self.logical_block_size {
            let logical_block_size = logical_block_size as usize;
            let offset = offset as usize;

            let offset_aligned_down = Self::round_down(offset, logical_block_size);

            let total_length = iovecs.iter().fold(0, |acc, e| acc + e.iov_len);

            let end = offset + total_length;
            let end_aligned_up = Self::round_up(end, logical_block_size);

            // Pad iovecs with dummy header and footer
            let mut iovecs: Vec<libc::iovec> = iovecs.iter().cloned().collect();
            let header =
                crate::new_aligned_iovec(offset - offset_aligned_down, logical_block_size)?;
            iovecs.insert(0, header);
            let footer = crate::new_aligned_iovec(end_aligned_up - end, logical_block_size)?;
            iovecs.push(footer);

            // SAFETY: FFI call with valid arguments
            let result = unsafe {
                libc::preadv(
                    self.fd as libc::c_int,
                    iovecs.as_ptr(),
                    iovecs.len() as libc::c_int,
                    offset_aligned_down as _,
                )
            };
            if result < 0 {
                return Err(AsyncIoError::ReadVectored(std::io::Error::last_os_error()));
            }

            self.completion_list.push_back((
                user_data,
                (result as usize - (header.iov_len + footer.iov_len)) as i32,
            ));
            self.eventfd.write(1).unwrap();

            return Ok(());
        }

        // SAFETY: FFI call with valid arguments
        let result = unsafe {
            libc::preadv(
                self.fd as libc::c_int,
                iovecs.as_ptr(),
                iovecs.len() as libc::c_int,
                offset,
            )
        };
        if result < 0 {
            return Err(AsyncIoError::ReadVectored(std::io::Error::last_os_error()));
        }

        self.completion_list.push_back((user_data, result as i32));
        self.eventfd.write(1).unwrap();

        Ok(())
    }

    fn write_vectored(
        &mut self,
        offset: libc::off_t,
        iovecs: &[libc::iovec],
        user_data: u64,
    ) -> AsyncIoResult<()> {
        if let Some(logical_block_size) = self.logical_block_size {
            let logical_block_size = logical_block_size as usize;

            let offset = offset as usize;
            let offset_aligned_down = Self::round_down(offset, logical_block_size);
            let offset_aligned_up = Self::round_up(offset, logical_block_size);

            let total_length = iovecs.iter().fold(0, |acc, e| acc + e.iov_len);

            let end = offset + total_length;
            let end_aligned_down = Self::round_down(end, logical_block_size);
            let end_aligned_up = Self::round_up(end, logical_block_size);

            // Read headers
            let header1 =
                crate::new_aligned_iovec(offset - offset_aligned_down, logical_block_size)?;
            let header2 = crate::new_aligned_iovec(offset_aligned_up - offset, logical_block_size)?;
            let mut header_iovecs: Vec<libc::iovec> = Vec::new();
            header_iovecs.push(header1);
            header_iovecs.push(header2);
            // SAFETY: FFI call with valid arguments
            let header_read_result = unsafe {
                libc::preadv(
                    self.fd as libc::c_int,
                    header_iovecs.as_ptr(),
                    header_iovecs.len() as libc::c_int,
                    offset_aligned_down as _,
                )
            };
            if header_read_result < 0 {
                return Err(AsyncIoError::ReadVectored(std::io::Error::last_os_error()));
            }

            // Read footers
            let footer1 = crate::new_aligned_iovec(end - end_aligned_down, logical_block_size)?;
            let footer2 = crate::new_aligned_iovec(end_aligned_up - end, logical_block_size)?;
            let mut footer_iovecs: Vec<libc::iovec> = Vec::new();
            footer_iovecs.push(footer1);
            footer_iovecs.push(footer2);
            // SAFETY: FFI call with valid arguments
            let footer_read_result = unsafe {
                libc::preadv(
                    self.fd as libc::c_int,
                    footer_iovecs.as_ptr(),
                    footer_iovecs.len() as libc::c_int,
                    end_aligned_down as _,
                )
            };
            if footer_read_result < 0 {
                return Err(AsyncIoError::ReadVectored(std::io::Error::last_os_error()));
            }

            // Pad iovecs with header1 and footer2
            let mut iovecs: Vec<libc::iovec> = iovecs.iter().cloned().collect();
            iovecs.insert(0, header1);
            iovecs.push(footer2);

            // SAFETY: FFI call with valid arguments
            let result = unsafe {
                libc::pwritev(
                    self.fd as libc::c_int,
                    iovecs.as_ptr(),
                    iovecs.len() as libc::c_int,
                    offset_aligned_down as _,
                )
            };
            if result < 0 {
                return Err(AsyncIoError::WriteVectored(std::io::Error::last_os_error()));
            }

            self.completion_list.push_back((
                user_data,
                (result as usize - (header1.iov_len + footer2.iov_len)) as i32,
            ));
            self.eventfd.write(1).unwrap();

            return Ok(());
        }
        // SAFETY: FFI call with valid arguments
        let result = unsafe {
            libc::pwritev(
                self.fd as libc::c_int,
                iovecs.as_ptr(),
                iovecs.len() as libc::c_int,
                offset,
            )
        };
        if result < 0 {
            return Err(AsyncIoError::WriteVectored(std::io::Error::last_os_error()));
        }

        self.completion_list.push_back((user_data, result as i32));
        self.eventfd.write(1).unwrap();

        Ok(())
    }

    fn fsync(&mut self, user_data: Option<u64>) -> AsyncIoResult<()> {
        // SAFETY: FFI call
        let result = unsafe { libc::fsync(self.fd as libc::c_int) };
        if result < 0 {
            return Err(AsyncIoError::Fsync(std::io::Error::last_os_error()));
        }

        if let Some(user_data) = user_data {
            self.completion_list.push_back((user_data, result));
            self.eventfd.write(1).unwrap();
        }

        Ok(())
    }

    fn next_completed_request(&mut self) -> Option<(u64, i32)> {
        self.completion_list.pop_front()
    }
}
