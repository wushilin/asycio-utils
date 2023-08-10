use tokio::io::{AsyncRead, AsyncSeek, SeekFrom, AsyncSeekExt, AsyncReadExt, AsyncWriteExt};
use std::pin::Pin;
use core::task::Poll;
use tokio::io::ReadBuf;
use std::error::Error;
use std::io::Read;
use std::io::Write;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObserverDecision {
    Continue,
    Abort,
}
/// Observe a stream's operation when user code is not involved.
/// For example: When you call EhRead.stream_to function, and you may want to calculate
/// the checksum (SHA1, SHA256 or MD5 along the way, you can observe the stream copy using the observer)
pub trait StreamObserver {
    /// Your observer's begin will be called before stream copy
    /// If you plan to reuse the observer, you can reset it here
    fn begin(&mut self) {
        // Default initialize does nothing
    }

    /// Before the upstream is read. If you want to abort, abort it here by
    /// overriding the before_read()
    fn before_read(&mut self) -> ObserverDecision {
        return ObserverDecision::Continue;
    }
    /// A chunk of data had been read from upstream, about to be written now
    /// You can intercept it by aborting it here
    fn before_write(&mut self, _: &[u8]) -> ObserverDecision {
        return ObserverDecision::Continue;
    }

    /// A chunk of data had been written to down stream.
    /// You can intercept it by aborting it here
    fn after_write(&mut self, _:&[u8]) -> ObserverDecision {
        return ObserverDecision::Continue;
    }

    /// The copy ended and `size` bytes had been copied
    /// If there is error, err with be Some(cause)
    /// Note, different from Result<usize, Box<dyn Error>>, the bytes copied is always given
    /// Even if it is zero
    fn end(&mut self, _:usize, _:Option<Box<&dyn Error>>) {

    }
}

struct DumbObserver;
impl StreamObserver for DumbObserver {
}
/// Enhanced Reader for std::io::Read
/// It provides convenient methods for exact reading without throwing error
/// It allow you to send it to writer
pub trait EhRead:Read {
    /// Try to fully read to fill the buffer, similar to read_exact, 
    /// However, this method never throw errors on error on EOF.
    /// On EOF, it also returns Ok(size) but size might be smaller than available buffer.
    /// When size is smaller than buffer size, it must be EOF.
    /// 
    /// Upon EOF, you may read again, but you will get EOF anyway with the EOF error.
    fn try_read_exact(&mut self, buffer: &mut [u8]) -> Result<usize, Box<dyn Error>> {
        let wanted = buffer.len();
        let mut copied:usize = 0;

        loop {
            let rr = self.read(&mut buffer[copied..])?;
            if rr == 0 {
                // EOF reached, return copied bytes so far. 
                // Caller upon seeing result shorter than expected can either:
                // a) declare EOF
                // b) call try_read_exact again but receive 0 bytes as result
                return Ok(copied);
            }
            copied = copied + rr;
            if copied >= wanted {
                return Ok(copied);
            }
        }
    }


    /// Skip bytes from the reader. Return the actual size skipped or the error.
    /// If EOF reached before skip is complete, UnexpectedEOF error is returned.
    /// On success, the size must be equal to the input bytes
    fn skip(&mut self, bytes: usize) -> Result<usize, Box<dyn Error>> {
        if bytes == 0 {
            return Ok(0);
        }
        let mut buffer = [0u8; 4096];
        let mut remaining = bytes;
        while remaining > 0 {
            let rr = self.try_read_exact(&mut buffer[..remaining])?;
            if rr == 0 {
                // EOF reached
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Insufficient bytes to skip").into());
            }
            remaining -= rr;
        }
        Ok(bytes)
    }

    /// Copy all content until EOF to Write. 
    /// Using buffer_size buffer. If not given, 4096 is used.
    /// If buffer_size is Some(0), 4096 is used
    /// 
    /// Return the number of bytes copied, or any error encountered. 
    /// 
    /// If error is EOF, then error is not returned and size would be 0.
    fn stream_to<W>(&mut self, w:&mut W, buffer_size:Option<usize>, observer:Option<Box<dyn StreamObserver>>) -> Result<usize, Box<dyn Error>> 
        where W:Write + Sized
    {
        let mut buffer_size = buffer_size.unwrap_or(4096);
        if buffer_size == 0 {
            buffer_size = 4096;
        }
        let mut buffer = vec![0u8; buffer_size];
        return self.stream_to_with_buffer(w, &mut buffer, observer);
    }

    /// Same as stream_to, but use externally provided buffer
    fn stream_to_with_buffer<W>(&mut self, w:&mut W, buffer:&mut[u8], observer:Option<Box<dyn StreamObserver>>) -> Result<usize, Box<dyn Error>>
        where W:Write+Sized {
        let mut observer = observer;
        let default_ob: Box<dyn StreamObserver> = Box::new(DumbObserver);
        let mut obs = observer.take().unwrap_or(default_ob);
        let mut copied:usize = 0;
        loop {
            let decision = obs.before_read();
            if decision == ObserverDecision::Abort {
                break;
            }
            let rr = self.read(buffer);
            if rr.is_err() {
                let err = rr.err().unwrap();
                obs.end(copied, Some(Box::new(&err)));
                return Err(err.into());
            }
            let rr = rr.unwrap();
            if rr == 0 {
                // EOF
                break;
            }
            let decision = obs.before_write(&buffer[0..rr]);
            if decision == ObserverDecision::Abort {
                break;
            }
            let wr = w.write_all(&buffer[0..rr]);
            if wr.is_err() {
                let err = wr.err().unwrap();
                obs.end(copied, Some(Box::new(&err)));
                return Err(err.into());
            }
            let decision = obs.after_write(&buffer[0..rr]);
            if decision == ObserverDecision::Abort {
                break;
            }
            copied += rr;
        }
        return Ok(copied);
    }


}


/// Blanked implementation for EhRead for all Read for free
/// 
/// You can use EhRead functions on all Read's implementations as long as 
/// you use this library and import EhRead trait.
impl <T> EhRead for T where T:Read{}
/// Undo reader supports unread(&[u8])
/// Useful when you are doing serialization/deserialization where you 
/// need to put data back (undo the read)
/// You can use UndoReader as if it is a normal AsyncRead
/// Additionally, UndoReader supports a limit as well. It would stop reading after limit is reached (EOF)

/// Example:
/// ```
/// // You can have rust code between fences inside the comments
/// // If you pass --test to `rustdoc`, it will even test it for you!
/// async fn do_test() -> Result<(), Box<dyn std::error::Error>> {
///     use tokio::io::{AsyncRead,AsyncSeek,AsyncReadExt, AsyncSeekExt};
///     let f = tokio::fs::File::open("input.txt").await?;
///     let mut my_undo = crate::asyncio_utils::UndoReader::new(f, Some(10)); // only read 10 bytes
///     let mut buff = vec![0u8; 10];
///     let read_count = my_undo.read(&mut buff).await?;
///     if read_count > 0 {
///         // inspect the data read check if it is ok
///         my_undo.unread(&mut buff[0..read_count]); // put all bytes back
///     }
///     let data = "XYZ".as_bytes();
///     my_undo.unread(&data);
///     // this should be 3 (the "XYZ" should be read here)
///     let second_read_count = my_undo.read(&mut buff).await?;
///     // this should be equal to read_count because it would have been reread here
///     let third_read_count = my_undo.read(&mut buff).await?;
///     // ...
///     Ok(())
/// }
/// ```
pub struct UndoReader<T>
    where T:AsyncRead + Unpin
{
    src: T,
    read_count: usize,
    limit: usize,
    buffer: Vec<Vec<u8>>
}

impl<T> UndoReader<T>
    where T:AsyncRead + Unpin
{
    /// Destruct this UndoReader. 
    /// 
    /// Returns the buffer that has been unread but has not been consumed as well as the raw AsyncRead
    /// Example:
    /// ```
    /// // initialize my_undo
    /// async fn do_test() -> Result<(), Box<dyn std::error::Error>> {
    ///     let f = tokio::fs::File::open("input.txt").await?;
    ///     let my_undo = crate::asyncio_utils::UndoReader::new(f, None);
    ///     let (remaining, raw) = my_undo.destruct();
    ///     // ...
    ///     Ok(())
    /// }
    /// // remaining is the bytes to be consumed.
    /// // raw is the raw AsyncRead
    /// ```
    /// 
    /// The UndoReader can't be used anymore after this call
    pub fn destruct(self) -> (Vec<u8>, T) {
        let count = self.count_unread();
        let mut resultv = vec![0u8; count];
        self.copy_into(&mut resultv); 
        return (resultv, self.src)
    }

    // Copy the remaining buffer to given bytes
    // Internal use only
    fn copy_into(&self, buf:&mut [u8]) -> usize{
        let mut copied = 0;
        for i in 0.. self.buffer.len() {
            let v = &self.buffer[ self.buffer.len() - i - 1];
            for i in 0..v.len() {
                buf[copied + i] = v[i];
            }
            copied += v.len();
        }
        return copied;
    }

    /// Get the limit of the UndoReader
    /// If the limit was None, this would be the usize's max value.
    pub fn limit(&self)->usize {
        self.limit
    }

    /// Count the number of bytes in the unread buffer
    pub fn count_unread(&self) -> usize {
        let mut result:usize = 0;
        for v in &self.buffer {
            result += v.len();
        }
        return result;
    }

    /// Create new UndoReader with limitation.
    /// If limit is None, `std::usize::MAX` will be used
    /// If limit is Some(limit:usize), the limit will be used
    pub fn new(src:T, limit:Option<usize>) -> UndoReader<T> {
        UndoReader {
            src, 
            limit: match limit {
                None => std::usize::MAX,
                Some(actual) => actual
            },
            read_count: 0,
            buffer: Vec::new()
        }
    }

    /// Put data for unread so it can be read again.
    /// 
    /// Reading of unread data does not count towards the limit because we assume you 
    /// unconsumed something you consumed in the first place. 
    /// 
    /// However, practically, you can arbitrarily unread any data. So the limit may 
    /// break the promise in such cases
    pub fn unread(&mut self, data:&[u8]) -> &mut Self {
        if data.len() > 0 {
            let mut new = vec![0u8;data.len()];
            for (index, payload) in data.iter().enumerate() {
                new[index] = *payload;
            }
            self.buffer.push(new);
        }
        return self;
    }
}


/// Implementation of AsyncRead for UndoReader
impl<T> AsyncRead for UndoReader<T>
    where T:AsyncRead + Unpin
{
    fn poll_read(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>, 
        data: &mut ReadBuf<'_>) -> Poll<Result<(), std::io::Error>> { 
        loop {
            let next = self.buffer.pop();
            match next {
                Some(bufdata) => {
                    if bufdata.len() == 0 {
                        continue;
                    }
                    // give this data out
                    let available = bufdata.len();
                    let remaining = data.remaining();
                    if available <= remaining {
                        data.put_slice(&bufdata);
                    } else {
                        data.put_slice(&bufdata[0..remaining]);
                        let left_over = &bufdata[remaining..];
                        let mut new_vec = vec![0u8;left_over.len()];
                        for (index, payload) in left_over.iter().enumerate() {
                            new_vec[index] = *payload;
                        }
                        self.buffer.push(new_vec);
                    }
                    return Poll::Ready(Ok(()));
                },
                None => {
                    break;
                }
            }
        }
        if self.read_count >= self.limit {
            // Mark EOF directly
            return Poll::Ready(Ok(()));
        }
        let ms = &mut *self;
        let p = Pin::new(&mut ms.src);
        let before_filled = data.filled().len();
        let result = p.poll_read(ctx, data);
        let after_filled = data.filled().len();
        let this_read = after_filled - before_filled;
        self.read_count += this_read;

        let overread = self.read_count > self.limit;
        if overread {
            let overread_count = self.read_count - self.limit;
            //undo overread portion
            data.set_filled(after_filled - overread_count);
            self.read_count = self.limit;
        }
        
        return result;
    }
}


/// An AsyncRead + AsyncSeek wrapper
/// It supports limit the bytes can be read.
/// Typically used when you want to read a specific segments from a file
/// 
/// Example
/// ```
/// 
/// async fn run_test() -> Result<(), Box<dyn std::error::Error>> {
///     use tokio::io::SeekFrom;
///     use tokio::io::{AsyncRead,AsyncSeek, AsyncReadExt, AsyncSeekExt};
///     let f = tokio::fs::File::open("input.txt").await?;
///     let read_from: u64 = 18; // start read from 18'th byte
///     let mut lsr = crate::asyncio_utils::LimitSeekerReader::new(f, Some(20)); // read up to 20 bytes
///     lsr.seek(SeekFrom::Start(read_from)); // do seek
/// 
///     let mut buf = vec![0u8; 1024];
///     lsr.read(&mut buf); // read it
///     return Ok(());
/// }
/// ```
pub struct LimitSeekerReader<T>
    where T:AsyncRead + AsyncSeek + Unpin
{
    src: T,
    read_count: usize,
    limit: usize,
}

/// Implement a limit reader for AsyncRead and AsyncSeek together. Typically a file
/// Note that if your seek does not affect total reads. You can seek with positive/negative
/// from current/begin of file/end of file, but it does not change the total bytes would be 
/// read from the reader.
/// 
/// This is a little bit weird though. Typically what you want to do is just seek before reading.
/// 
/// This is useful when you want to service Http Get with Range requests.
/// 
/// You open tokio::fs::File 
/// you seek the position
/// you set limit on number of bytes to read
/// you start reading and serving.
/// 
impl<T> LimitSeekerReader<T>
    where T:AsyncRead + AsyncSeek + Unpin
{
    /// Destruct the LimitSeekerReader and get the bytes read so far and the original reader
    /// Returns the size read and the original reader. 
    /// 
    /// You can't use the LimitSeekerReader after this call
    pub fn destruct(self) -> (usize, T) {
        (self.read_count, self.src)
    }

    /// Create new LimitSeekerReader from another AsyncRead + AsyncSeek (typically file)
    /// 
    /// Argument src is the underlying reader + seeker
    /// limit is the byte limit. Node that the limit can be
    ///     Some(limit)
    ///     None -> No limit (std::usize::MAX)
    pub fn new(src:T, limit:Option<usize>) -> LimitSeekerReader<T> {
        LimitSeekerReader {
            src, 
            limit: {
                match limit {
                    None => std::usize::MAX,
                    Some(actual_limit) => actual_limit
                }
            },
            read_count: 0
        }
    }
}

/// Implementation of AsyncRead
impl<T> AsyncRead for LimitSeekerReader<T>
    where T:AsyncRead + AsyncSeek + Unpin
{
    fn poll_read(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>, 
        data: &mut ReadBuf<'_>) -> Poll<Result<(), std::io::Error>> { 
        if self.read_count >= self.limit {
            // Mark EOF directly
            return Poll::Ready(Ok(()));
        }
        let ms = &mut *self;
        let p = Pin::new(&mut ms.src);
        let before_filled = data.filled().len();
        let result = p.poll_read(ctx, data);
        let after_filled = data.filled().len();
        let this_read = after_filled - before_filled;
        self.read_count += this_read;

        let overread = self.read_count > self.limit;
        if overread {
            let overread_count = self.read_count - self.limit;
            //undo overread portion
            data.set_filled(after_filled - overread_count);
            self.read_count = self.limit;
        }
        
        return result;
    }
}


/// Implementation of AsyncSeek
impl<T> AsyncSeek for LimitSeekerReader<T>
    where T:AsyncRead + AsyncSeek + Unpin
{
    fn start_seek(mut self: Pin<&mut Self>, from: SeekFrom) -> Result<(), std::io::Error> {
        let ms = &mut *self;
        let p = Pin::new(&mut ms.src);
        return p.start_seek(from);
    }

    fn poll_complete(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>) -> Poll<Result<u64, std::io::Error>> { 
        let ms = &mut *self;
        let p = Pin::new(&mut ms.src);
        return p.poll_complete(ctx);
    }
}


/// Pure implementation for LimitReader to restrict number of bytes can be read
/// Useful when you want to read stream but want to end early no matter what
/// 
/// E.g. you can't accept more than 20MiB as HTTP Post body, you can limit it here
/// Example:
/// ```
/// async fn do_test() -> Result<(), Box<dyn std::error::Error>> {
///     use tokio::io::{AsyncReadExt};
///     let mut f = tokio::fs::File::open("input.txt").await?;
///     let mut reader = crate::asyncio_utils::LimitReader::new(f, Some(18)); // only read at most 18 bytes
/// 
///     let mut buf = vec![0u8; 2096];
///     reader.read(&mut buf).await?;
///     return Ok(());
/// }
/// ```
pub struct LimitReader<T>
    where T:AsyncRead + Unpin
{
    src: T,
    read_count: usize,
    limit: usize,
}


/// Implementation of AysncRead for LimitReader
impl<T> LimitReader<T>
    where T:AsyncRead + Unpin
{
    /// Create new LimitReader from another AsyncRead (typically File or Stream)
    /// 
    /// Argument src is the underlying reader
    /// limit is the byte limit. Node that the limit can be
    ///     Some(limit) -> The limit is set
    ///     None -> No limit (std::usize::MAX)
    pub fn new(src:T, limit:Option<usize>) -> LimitReader<T> {
        LimitReader {
            src, 
            limit: {
                match limit {
                    None => std::usize::MAX,
                    Some(actual_limit) => actual_limit
                }
            },
            read_count: 0
        }
    }

    /// Destruct the LimitReader and get the total read bytes and the original reader
    /// 
    /// Takes the ownership and You can't use the LimitReader after this call
    pub fn destruct(self) -> (usize, T) {
        (self.read_count, self.src)
    }
}

/// Implementation of AsyncRead
impl<T> AsyncRead for LimitReader<T>
    where T:AsyncRead + Unpin
{
    fn poll_read(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>, 
        data: &mut ReadBuf<'_>) -> Poll<Result<(), std::io::Error>> { 
        if self.read_count >= self.limit {
            // Mark EOF directly
            return Poll::Ready(Ok(()));
        }
        let ms = &mut *self;
        let p = Pin::new(&mut ms.src);
        let before_filled = data.filled().len();
        let result = p.poll_read(ctx, data);
        let after_filled = data.filled().len();
        let this_read = after_filled - before_filled;
        self.read_count += this_read;

        let overread = self.read_count > self.limit;
        if overread {
            let overread_count = self.read_count - self.limit;
            //undo overread portion
            data.set_filled(after_filled - overread_count);
            self.read_count = self.limit;
        }
        
        return result;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_undo() {
        let file = tokio::fs::File::open("test.data").await.unwrap();
        let mut undor = UndoReader::new(file, Some(10));
        let mut buf = [0u8; 1024];
        let undo = "XXX".as_bytes();
        let undo1 = "YYY".as_bytes();
        undor.unread(undo);
        undor.unread(undo1);
        let rr = undor.read(&mut buf).await.unwrap();
        // Note Unread bytes does not count as actual consumption
        assert_eq!(&buf[0..rr], "YYY".as_bytes()); //undo read is priority
        let rr = undor.read(&mut buf).await.unwrap();
        // Note Unread bytes does not count as actual consumption
        assert_eq!(&buf[0..rr], "XXX".as_bytes()); //undo read is priority
        let rr = undor.read(&mut buf).await.unwrap();
        assert_eq!(&buf[0..rr], "123456789\n".as_bytes());

        //assert_eq!(result, 4);
    }
    #[tokio::test]
    async fn test_limit() {
        let file = tokio::fs::File::open("test.data").await.unwrap();
        let mut limitr = LimitReader::new(file, Some(10));
        let mut buf = [0u8; 1024];
        let rr = limitr.read(&mut buf).await.unwrap();
        assert_eq!(&buf[0..rr], "123456789\n".as_bytes());
        let rr = limitr.read(&mut buf).await.unwrap();
        assert_eq!(&buf[0..rr], "".as_bytes());
        //assert_eq!(result, 4);
    }

    #[tokio::test]
    async fn test_seek() {
        let file = tokio::fs::File::open("test.data").await.unwrap();
        let mut limitr = LimitSeekerReader::new(file, Some(10));
        limitr.seek(SeekFrom::Current(13)).await.unwrap();
        let mut buf = [0u8; 1024];
        let rr = limitr.read(&mut buf).await.unwrap();
        assert_eq!(&buf[0..rr], "456789\n123".as_bytes());
        let rr = limitr.read(&mut buf).await.unwrap();
        assert_eq!(&buf[0..rr], "".as_bytes());
        //assert_eq!(result, 4);
    }
}
