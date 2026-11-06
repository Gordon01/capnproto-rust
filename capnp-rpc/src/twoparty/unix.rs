//! Native FD passing over Tokio Unix sockets.
//!
//! Uses standard Cap'n Proto framing with `SCM_RIGHTS` ancillary data. Do not
//! wrap the socket in a byte-only reader or writer: those discard ancillary data.

use super::{Connection, Sender as MessageSender, VatId, VatNetwork};
use capnp::capability::Promise;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use futures::channel::{mpsc, oneshot};
use futures::{AsyncRead, StreamExt, TryFutureExt};
use rustix::net::{
    recvmsg, sendmsg, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags,
};
use std::cell::RefCell;
use std::io::{self, IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{ready, Context, Poll};
use tokio::io::Interest;
use tokio::net::UnixStream;

const MAX_FDS: usize = 253;

// Darwin leaks descriptor table entries when SCM_RIGHTS is truncated, so reserve
// room for more FDs than a peer can usefully send:
// https://gist.github.com/kentonv/bc7592af98c68ba2738f4436920868dc
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "visionos",
))]
const RECV_FDS: usize = 1024;
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "visionos",
)))]
const RECV_FDS: usize = MAX_FDS;

/// Read half of an FD-aware connection, created by [`VatNetwork::new_unix`].
///
/// Reads never run ahead of the message parser, keeping ancillary data associated
/// with the message whose bytes carried it.
pub struct UnixRead {
    socket: Rc<UnixStream>,
    fds: Rc<RefCell<Vec<OwnedFd>>>,
    max_fds: usize,
}

impl AsyncRead for UnixRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            ready!(self.socket.poll_read_ready(cx))?;
            let result = self.socket.try_io(Interest::READABLE, || {
                let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(RECV_FDS))];
                let mut ancillary = RecvAncillaryBuffer::new(&mut space);
                let flags = RecvFlags::DONTWAIT;
                #[cfg(any(target_os = "linux", target_os = "android"))]
                let flags = flags | RecvFlags::CMSG_CLOEXEC;
                let result = recvmsg(
                    self.socket.as_ref(),
                    &mut [IoSliceMut::new(buffer)],
                    &mut ancillary,
                    flags,
                )?;
                let mut received = self.fds.borrow_mut();
                for message in ancillary.drain() {
                    if let RecvAncillaryMessage::ScmRights(fds) = message {
                        for fd in fds {
                            #[cfg(not(any(target_os = "linux", target_os = "android")))]
                            rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)?;
                            if received.len() < self.max_fds {
                                received.push(fd);
                            }
                        }
                    }
                }
                Ok(result.bytes)
            });
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return Poll::Ready(result),
            }
        }
    }
}

enum Item {
    Message(
        Rc<Builder<HeapAllocator>>,
        Vec<Rc<OwnedFd>>,
        oneshot::Sender<()>,
    ),
    Done(capnp::Result<()>, oneshot::Sender<()>),
}

#[derive(Clone)]
pub(super) struct Sender(mpsc::UnboundedSender<Item>);

impl Sender {
    pub(super) fn send(
        &mut self,
        message: Rc<Builder<HeapAllocator>>,
        fds: Vec<Rc<OwnedFd>>,
    ) -> Promise<(), capnp::Error> {
        let (complete, receiver) = oneshot::channel();
        if self
            .0
            .unbounded_send(Item::Message(message, fds, complete))
            .is_err()
        {
            return Promise::err(capnp::Error::disconnected(
                "Unix write queue has terminated".into(),
            ));
        }
        Promise::from_future(receiver.map_err(crate::canceled_to_error))
    }

    pub(super) fn terminate(&mut self, result: capnp::Result<()>) -> Promise<(), capnp::Error> {
        let (complete, receiver) = oneshot::channel();
        if self.0.unbounded_send(Item::Done(result, complete)).is_err() {
            return Promise::err(capnp::Error::disconnected(
                "Unix write queue has terminated".into(),
            ));
        }
        Promise::from_future(receiver.map_err(crate::canceled_to_error))
    }
}

async fn write_message(
    socket: &UnixStream,
    message: &Builder<HeapAllocator>,
    fds: &[Rc<OwnedFd>],
) -> capnp::Result<()> {
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "visionos",
    ))]
    rustix::net::sockopt::set_socket_nosigpipe(socket, true).map_err(io::Error::from)?;
    if fds.len() > MAX_FDS {
        return Err(capnp::Error::failed(
            "Too many attached file descriptors".into(),
        ));
    }

    let bytes = capnp::serialize::write_message_to_words(message);
    let borrowed: Vec<_> = fds.iter().map(|fd| fd.as_fd()).collect();
    let mut offset = 0;
    while offset < bytes.len() {
        let count = socket
            .async_io(Interest::WRITABLE, || {
                // Rebuild after EAGAIN/EINTR; sendmsg may consume the ancillary buffer.
                let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
                let mut ancillary = SendAncillaryBuffer::new(&mut space);
                if offset == 0
                    && !borrowed.is_empty()
                    && !ancillary.push(SendAncillaryMessage::ScmRights(&borrowed))
                {
                    return Err(io::Error::other("Cannot encode attached file descriptors"));
                }
                let flags = SendFlags::DONTWAIT;
                #[cfg(not(any(
                    target_os = "macos",
                    target_os = "ios",
                    target_os = "tvos",
                    target_os = "watchos",
                    target_os = "visionos",
                )))]
                let flags = flags | SendFlags::NOSIGNAL;
                Ok(sendmsg(
                    socket,
                    &[IoSlice::new(&bytes[offset..])],
                    &mut ancillary,
                    flags,
                )?)
            })
            .await;
        match count {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
            Ok(0) => {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "Sending RPC message").into())
            }
            Ok(count) => offset += count,
        }
    }
    Ok(())
}

impl VatNetwork<UnixRead> {
    /// Creates an FD-aware two-party network over a Tokio Unix socket.
    ///
    /// `max_fds_per_message` limits accepted descriptors (at most 253). Excess
    /// descriptors are closed; zero disables receiving FDs. Missing descriptors
    /// are permitted by the RPC protocol, so applications must handle `get_fd()`
    /// returning `None`. Never read or write the socket outside this network.
    ///
    /// Linux/Android receive descriptors atomically with close-on-exec. Other
    /// Unix platforms set it immediately after receipt; applications on those
    /// platforms must synchronize concurrent fork/exec themselves.
    pub fn new_unix(
        socket: UnixStream,
        side: VatId,
        receive_options: ReaderOptions,
        max_fds_per_message: u8,
    ) -> Self {
        let socket = Rc::new(socket);
        let fds = Rc::new(RefCell::new(Vec::new()));
        let input = UnixRead {
            socket: socket.clone(),
            fds: fds.clone(),
            max_fds: usize::from(max_fds_per_message).min(MAX_FDS),
        };
        let (tx, mut rx) = mpsc::unbounded();
        let writer = async move {
            while let Some(item) = rx.next().await {
                match item {
                    Item::Message(message, fds, complete) => {
                        write_message(&socket, &message, &fds).await?;
                        let _ = complete.send(());
                    }
                    Item::Done(result, complete) => {
                        let _ = complete.send(());
                        return result;
                    }
                }
            }
            Ok(())
        };
        let network = Self::new_internal(
            input,
            MessageSender::Unix(Sender(tx)),
            writer,
            side,
            receive_options,
        );
        if let Some(Connection { inner }) = &network.connection {
            inner.borrow_mut().incoming_fds = Some(fds);
        }
        network
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VatNetwork as _;
    use std::io::Read;
    use std::os::unix::net::UnixStream as StdUnixStream;
    use std::time::Duration;

    fn run(future: impl std::future::Future<Output = capnp::Result<()>>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(10), future)
                    .await
                    .unwrap()
                    .unwrap();
            });
    }

    #[test]
    fn limited_fds_partial_writes_and_message_boundaries() {
        run(async {
            let (a, b) = UnixStream::pair()?;
            rustix::net::sockopt::set_socket_send_buffer_size(&a, 4096).map_err(io::Error::from)?;
            let mut a = VatNetwork::new_unix(a, VatId::Client, ReaderOptions::new(), 3);
            let mut b = VatNetwork::new_unix(b, VatId::Server, ReaderOptions::new(), 1);
            let mut sender = a.connect(VatId::Server).unwrap();
            let mut receiver = b.connect(VatId::Client).unwrap();
            let work = async move {
                let payload = vec![42; 256 * 1024];
                for count in [3, 0, 2] {
                    let mut message = sender.new_outgoing_message(0);
                    message
                        .get_body()?
                        .set_as::<capnp::data::Owned>(&payload[..])?;
                    let mut peers = Vec::new();
                    let mut fds = Vec::new();
                    for _ in 0..count {
                        let (fd, mut peer) = StdUnixStream::pair()?;
                        peer.set_nonblocking(true)?;
                        assert_eq!(
                            peer.read(&mut [0]).unwrap_err().kind(),
                            io::ErrorKind::WouldBlock
                        );
                        fds.push(Rc::new(OwnedFd::from(fd)));
                        peers.push(peer);
                    }
                    message.set_fds(fds);
                    // Dropping the completion promise must not cancel the queued write.
                    drop(message.send());
                    let received = receiver.receive_incoming_message().await?.unwrap();
                    assert_eq!(
                        received.get_body()?.get_as::<capnp::data::Reader>()?,
                        payload
                    );
                    assert!(received.take_fd(1).is_none());
                    assert!(received.take_fd(255).is_none());
                    if count > 0 {
                        let fd = received.take_fd(0).unwrap();
                        assert!(received.take_fd(0).is_none());
                        assert!(rustix::io::fcntl_getfd(&fd)
                            .map_err(io::Error::from)?
                            .contains(rustix::io::FdFlags::CLOEXEC));
                        assert_eq!(
                            peers[0].read(&mut [0]).unwrap_err().kind(),
                            io::ErrorKind::WouldBlock
                        );
                        drop(fd);
                        assert_eq!(peers[0].read(&mut [0])?, 0);
                        for peer in &mut peers[1..] {
                            assert_eq!(peer.read(&mut [0])?, 0);
                        }
                    } else {
                        assert!(received.take_fd(0).is_none());
                    }
                }
                Ok(())
            };
            let drivers =
                futures::future::try_join(a.drive_until_shutdown(), b.drive_until_shutdown());
            futures::pin_mut!(work, drivers);
            match futures::future::select(work, drivers).await {
                futures::future::Either::Left((result, _)) => result,
                futures::future::Either::Right((result, _)) => {
                    result?;
                    panic!("transport stopped before messages arrived");
                }
            }
        });
    }

    #[test]
    fn failed_write_is_reported() {
        run(async {
            let (socket, peer) = UnixStream::pair()?;
            drop(peer);
            let mut message = Builder::new_default();
            message.set_root::<capnp::data::Owned>(&b"test"[..])?;
            assert!(write_message(&socket, &message, &[]).await.is_err());
            Ok(())
        });
    }
}
