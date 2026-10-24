use std::io;

/// Retains only an unfinished native write, never bytes reported as accepted.
#[derive(Default)]
pub struct WriteRequest {
    bytes: Vec<u8>,
}

impl WriteRequest {
    pub fn write(
        &mut self,
        bytes: &[u8],
        limit: usize,
        mut write: impl FnMut(&[u8]) -> io::Result<usize>,
    ) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if !self.bytes.is_empty() {
            // A cancelled caller may supply a different buffer. Finish the
            // old native call, but never report its count for the new payload.
            if bytes.starts_with(&self.bytes) {
                return self.complete(write);
            }
            self.complete(&mut write)?;
        }
        self.bytes
            .extend_from_slice(&bytes[..bytes.len().min(limit)]);
        self.complete(write)
    }

    pub fn flush(&mut self, write: impl FnMut(&[u8]) -> io::Result<usize>) -> io::Result<()> {
        if !self.bytes.is_empty() {
            self.complete(write)?;
        }
        Ok(())
    }

    pub const fn is_pending(&self) -> bool {
        !self.bytes.is_empty()
    }

    fn complete(&mut self, mut write: impl FnMut(&[u8]) -> io::Result<usize>) -> io::Result<usize> {
        let result = match write(&self.bytes) {
            Ok(0) => Err(io::ErrorKind::WouldBlock.into()),
            Ok(count) if count > self.bytes.len() => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native write exceeded the submitted buffer",
            )),
            result => result,
        };
        if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
            self.bytes.clear();
        }
        result
    }
}
