use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use sqld_libsql_bindings::rusqlite::{self, OpenFlags};

use crate::frame::{Frame, FrameNo};

pub use error::Error;
use hook::{
    InjectorHook, InjectorHookCtx, INJECTOR_METHODS, LIBSQL_INJECT_FATAL, LIBSQL_INJECT_OK,
    LIBSQL_INJECT_OK_TXN,
};

mod error;
mod headers;
mod hook;

#[derive(Debug)]
pub enum InjectError {}

pub type FrameBuffer = Arc<Mutex<VecDeque<Frame>>>;

pub struct Injector {
    /// The injector is in a transaction state
    is_txn: bool,
    /// Buffer for holding current transaction frames
    buffer: FrameBuffer,
    /// Maximum capacity of the frame buffer
    capacity: usize,
    /// Injector connection
    // connection must be dropped before the hook context
    connection: Arc<Mutex<sqld_libsql_bindings::Connection<InjectorHook>>>,
}

/// Methods from this trait are called before and after performing a frame injection.
/// This trait trait is used to record the last committed frame_no to the log.
/// The implementer can persist the pre and post commit frame no, and compare them in the event of
/// a crash; if the pre and post commit frame_no don't match, then the log may be corrupted.
impl Injector {
    pub fn new(path: impl AsRef<Path>, buffer_capacity: usize, auto_checkpoint: u32) -> Result<Self, Error> {
        let buffer = FrameBuffer::default();
        let ctx = InjectorHookCtx::new(buffer.clone());

        let connection = sqld_libsql_bindings::Connection::open(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            &INJECTOR_METHODS,
            // safety: hook is dropped after connection
            ctx,
            auto_checkpoint,
        )?;

        Ok(Self {
            is_txn: false,
            buffer,
            capacity: buffer_capacity,
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Inject on frame into the log. If this was a commit frame, returns Ok(Some(FrameNo)).
    pub fn inject_frame(&mut self, frame: Frame) -> Result<Option<FrameNo>, Error> {
        let frame_close_txn = frame.header().size_after != 0;
        self.buffer.lock().push_back(frame);
        if frame_close_txn || self.buffer.lock().len() >= self.capacity {
            if !self.is_txn {
                self.begin_txn()?;
            }
            return self.flush();
        }

        Ok(None)
    }

    /// Flush the buffer to libsql WAL.
    /// Trigger a dummy write, and flush the cache to trigger a call to xFrame. The buffer's frame
    /// are then injected into the wal.
    fn flush(&mut self) -> Result<Option<FrameNo>, Error> {
        let lock = self.buffer.lock();
        // the frames in the buffer are either monotonically increasing (log) or decreasing
        // (snapshot). Either way, we want to find the biggest frameno we're about to commit, and
        // that is either the front or the back of the buffer
        let last_frame_no = match lock.back().zip(lock.front()) {
            Some((b, f)) => f.header().frame_no.max(b.header().frame_no),
            None => {
                tracing::trace!("nothing to inject");
                return Ok(None);
            }
        };

        drop(lock);

        let connection = self.connection.lock();
        // use prepare cached to avoid parsing the same statement over and over again.
        let mut stmt =
            connection.prepare_cached("INSERT INTO libsql_temp_injection VALUES (42)")?;

        // We execute the statement, and then force a call to xframe if necesacary. If the execute
        // succeeds, then xframe wasn't called, in this case, we call cache_flush, and then process
        // the error.
        // It is unexpected that execute flushes, but it is possible, so we handle that case.
        match stmt.execute(()).and_then(|_| connection.cache_flush()) {
            Ok(_) => panic!("replication hook was not called"),
            Err(e) => {
                if let Some(e) = e.sqlite_error() {
                    if e.extended_code == LIBSQL_INJECT_OK {
                        // refresh schema
                        connection.pragma_update(None, "writable_schema", "reset")?;
                        if let Err(e) = connection.execute("COMMIT", ()) {
                            if !matches!(e.sqlite_error(), Some(rusqlite::ffi::Error{ extended_code, .. }) if *extended_code == 201)
                            {
                                tracing::error!("injector failed to commit: {e}");
                                return Err(Error::FatalInjectError);
                            }
                        }
                        self.is_txn = false;
                        assert!(self.buffer.lock().is_empty());
                        return Ok(Some(last_frame_no));
                    } else if e.extended_code == LIBSQL_INJECT_OK_TXN {
                        self.is_txn = true;
                        assert!(self.buffer.lock().is_empty());
                        return Ok(None);
                    } else if e.extended_code == LIBSQL_INJECT_FATAL {
                        return Err(Error::FatalInjectError);
                    }
                }

                Err(Error::FatalInjectError)
            }
        }
    }

    fn begin_txn(&mut self) -> Result<(), Error> {
        let conn = self.connection.lock();
        let mut stmt = conn.prepare_cached("BEGIN IMMEDIATE")?;
        stmt.execute(())?;
        // we create a dummy table. This table MUST not be persisted, otherwise the replica schema
        // would differ with the primary's.
        let mut stmt = conn.prepare_cached("CREATE TABLE IF NOT EXISTS libsql_temp_injection (x)")?;
        stmt.execute(())?;

        Ok(())
    }

    pub fn clear_buffer(&mut self) {
        self.buffer.lock().clear()
    }
}

#[cfg(test)]
mod test {
    use std::mem::size_of;

    use crate::frame::FrameBorrowed;

    use super::*;
    /// this this is generated by creating a table test, inserting 5 rows into it, and then
    /// truncating the wal file of it's header.
    const WAL: &[u8] = include_bytes!("../../assets/test/test_wallog");

    fn wal_log() -> impl Iterator<Item = Frame> {
        WAL.chunks(size_of::<FrameBorrowed>()).map(|b| Frame::try_from(b).unwrap())
    }

    #[test]
    fn test_simple_inject_frames() {
        let temp = tempfile::tempdir().unwrap();

        let mut injector = Injector::new(temp.path().join("data"), 10, 10000).unwrap();
        let mut log = wal_log();
        while let Some(frame) = log.next() {
            injector.inject_frame(frame.into()).unwrap();
        }

        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();

        conn.query_row("SELECT COUNT(*) FROM test", (), |row| {
            assert_eq!(row.get::<_, usize>(0).unwrap(), 5);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_inject_frames_split_txn() {
        let temp = tempfile::tempdir().unwrap();

        // inject one frame at a time
        let mut injector = Injector::new(temp.path().join("data"), 1, 10000).unwrap();
        let mut log = wal_log();
        while let Some(frame) = log.next() {
            injector.inject_frame(frame.into()).unwrap();
        }

        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();

        conn.query_row("SELECT COUNT(*) FROM test", (), |row| {
            assert_eq!(row.get::<_, usize>(0).unwrap(), 5);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_inject_partial_txn_isolated() {
        let temp = tempfile::tempdir().unwrap();

        // inject one frame at a time
        let mut injector = Injector::new(temp.path().join("data"), 10, 1000).unwrap();
        let mut frames = wal_log();

        assert!(injector
            .inject_frame(frames.next().unwrap().into())
            .unwrap()
            .is_none());
        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();
        assert!(conn
            .query_row("SELECT COUNT(*) FROM test", (), |_| Ok(()))
            .is_err());

        while injector
            .inject_frame(frames.next().unwrap().into())
            .unwrap()
            .is_none()
        {}

        // reset schema
        conn.pragma_update(None, "writable_schema", "reset")
            .unwrap();
        conn.query_row("SELECT COUNT(*) FROM test", (), |_| Ok(()))
            .unwrap();
    }
}
