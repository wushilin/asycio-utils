use tokio::io::{AsyncRead, AsyncSeek, SeekFrom, AsyncSeekExt, AsyncReadExt, AsyncWriteExt};
use std::pin::Pin;
use core::task::Poll;
use tokio::io::ReadBuf;
// Undo reader supports unread(&[u8])
// Useful when you are doing serialization/deserialization where you 
// need to put data back (undo the read)
// You can use UndoReader as if it is a normal AsyncRead
// Additionally, UndoReader supports a limit as well. It would stop reading after limit is reached (EOF)

// Example:
/// ```
/// // You can have rust code between fences inside the comments
/// // If you pass --test to `rustdoc`, it will even test it for you!
/// use asyncio_utils::UndoReader;
/// let input = tokio::fs::File::open("test.data").unwrap();
/// let first_10_bytes_reader = UndoReader::wrap(input, Some(10)); // limit to first 10 bytes, EOF afterwards
/// let my_data = first_10_bytes_reader.read(buf).await?;
/// // use my_data
/// 
/// let read_every_thing = UndoReader::wrap(input, None); // No limit (usize::max actually)
/// 
/// first_10_bytes_reader.unread(&my_data[4..7]); // Put back the 3 bytes back to the undo reader
/// 
/// first_10_bytes_reader.read(&mut buf); // definitely will be the 3 bytes you put back just now. Unless your buffer is too small
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
    /// let (remaining, raw) = my_undo.destruct();
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
pub struct LimitReader<T>
    where T:AsyncRead + Unpin
{
    src: T,
    read_count: usize,
    limit: usize,
}


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
