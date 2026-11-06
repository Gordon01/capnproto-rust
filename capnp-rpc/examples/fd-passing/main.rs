#[cfg(target_os = "linux")]
capnp::generated_code!(pub mod buffer_capnp);

#[cfg(target_os = "linux")]
mod demo {
    use crate::buffer_capnp::{buffer, exchange};
    use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};
    use futures::FutureExt;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::rc::Rc;
    use tokio::net::UnixStream;

    pub(crate) const CONTENT: &[u8] = b"Hello through a memfd!";

    struct Buffer(File);

    impl buffer::Server for Buffer {
        async fn read(
            self: Rc<Self>,
            _: buffer::ReadParams,
            mut results: buffer::ReadResults,
        ) -> capnp::Result<()> {
            use std::os::unix::fs::FileExt;
            let mut data = vec![0; self.0.metadata()?.len() as usize];
            self.0.read_exact_at(&mut data, 0)?;
            results.get().set_data(&data);
            Ok(())
        }
    }

    pub(crate) fn new_buffer(data: &[u8]) -> capnp::Result<buffer::Client> {
        let fd = rustix::fs::memfd_create(
            "capnp-buffer",
            rustix::fs::MemfdFlags::CLOEXEC | rustix::fs::MemfdFlags::ALLOW_SEALING,
        )
        .map_err(std::io::Error::from)?;
        let mut file = File::from(fd);
        file.write_all(data)?;
        rustix::fs::fcntl_add_seals(
            &file,
            rustix::fs::SealFlags::SHRINK
                | rustix::fs::SealFlags::GROW
                | rustix::fs::SealFlags::WRITE
                | rustix::fs::SealFlags::SEAL,
        )
        .map_err(std::io::Error::from)?;
        let fd = file.try_clone()?.into();
        Ok(capnp_rpc::new_client_with_fd(Buffer(file), fd))
    }

    pub(crate) struct Exchange(pub(crate) buffer::Client);

    impl exchange::Server for Exchange {
        async fn get(
            self: Rc<Self>,
            _: exchange::GetParams,
            mut results: exchange::GetResults,
        ) -> capnp::Result<()> {
            results.get().set_buffer(self.0.clone());
            Ok(())
        }

        async fn echo(
            self: Rc<Self>,
            params: exchange::EchoParams,
            mut results: exchange::EchoResults,
        ) -> capnp::Result<()> {
            let buffer = params.get()?.get_buffer()?;
            if let Some(fd) = capnp_rpc::get_fd(&buffer).await? {
                assert!(rustix::io::fcntl_getfd(fd.as_ref())
                    .map_err(std::io::Error::from)?
                    .contains(rustix::io::FdFlags::CLOEXEC));
            }
            results.get().set_buffer(buffer);
            Ok(())
        }
    }

    pub(crate) fn connect(
        socket: UnixStream,
        max_fds: u8,
        bootstrap: Option<exchange::Client>,
    ) -> RpcSystem<Side> {
        let side = if bootstrap.is_some() {
            Side::Server
        } else {
            Side::Client
        };
        RpcSystem::new(
            Box::new(twoparty::VatNetwork::new_unix(
                socket,
                side,
                Default::default(),
                max_fds,
            )),
            bootstrap.map(|client| client.client),
        )
    }

    pub(crate) async fn read_buffer(buffer: &buffer::Client) -> capnp::Result<Vec<u8>> {
        if let Some(fd) = capnp_rpc::get_fd(buffer).await? {
            let mut file = File::from(fd.try_clone()?);
            file.seek(SeekFrom::Start(0))?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            Ok(bytes)
        } else {
            Ok(buffer
                .read_request()
                .send()
                .promise
                .await?
                .get()?
                .get_data()?
                .to_vec())
        }
    }

    pub(crate) async fn run() -> capnp::Result<()> {
        let (client_socket, server_socket) = UnixStream::pair()?;
        let bootstrap = capnp_rpc::new_client(Exchange(new_buffer(CONTENT)?));
        let server = connect(server_socket, 8, Some(bootstrap));
        let mut client = connect(client_socket, 8, None);
        let exchange: exchange::Client = client.bootstrap(Side::Server);
        let work = async move {
            let response = exchange.get_request().send().promise.await?;
            let buffer = response.get()?.get_buffer()?;
            let fd = capnp_rpc::get_fd(&buffer)
                .await?
                .expect("Unix transport must pass the FD");
            let bytes = read_buffer(&buffer).await?;
            assert_eq!(bytes, CONTENT);
            println!(
                "Received memfd over Cap'n Proto RPC: {}",
                String::from_utf8_lossy(&bytes)
            );
            drop(fd);
            Ok(())
        };
        let drivers = futures::future::try_join(client, server).map(|result| result.map(|_| ()));
        futures::pin_mut!(work, drivers);
        match futures::future::select(work, drivers).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right((result, _)) => {
                result?;
                Err(capnp::Error::disconnected(
                    "RPC stopped before the transfer finished".into(),
                ))
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(demo::run())?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("This memfd example requires Linux.");
    std::process::exit(1);
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{buffer_capnp::exchange, demo::*};
    use capnp::capability::FromClientHook;
    use capnp_rpc::rpc_twoparty_capnp::Side;
    use std::rc::Rc;
    use std::time::Duration;
    use tokio::net::UnixStream;

    fn run_test(future: impl std::future::Future<Output = capnp::Result<()>>) {
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
    fn memfd_example() {
        run_test(run());
    }

    #[test]
    fn local_fd_and_clone_lifetime() {
        run_test(async {
            let buffer = new_buffer(CONTENT)?;
            let clone = buffer.clone();
            let fd = capnp_rpc::get_fd(&buffer).await?.unwrap();
            assert!(Rc::ptr_eq(&fd, &capnp_rpc::get_fd(&clone).await?.unwrap()));
            drop(buffer);
            drop(clone);
            let mut file = std::fs::File::from(fd.try_clone()?);
            use std::io::{Read, Seek, SeekFrom};
            file.seek(SeekFrom::Start(0))?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            assert_eq!(bytes, CONTENT);
            Ok(())
        });
    }

    #[test]
    fn broken_cap_has_no_fd() {
        run_test(async {
            let cap = capnp_rpc::new_broken_cap(capnp::Error::failed("broken".into()));
            assert!(capnp_rpc::get_fd(&cap).await?.is_none());
            Ok(())
        });
    }

    async fn transfer(max_fds: u8, promised: bool) -> capnp::Result<()> {
        let (a, b) = UnixStream::pair()?;
        let server_buffer = new_buffer(CONTENT)?;
        let server_buffer = if promised {
            capnp_rpc::new_future_client(async move {
                tokio::task::yield_now().await;
                Ok(server_buffer)
            })
        } else {
            server_buffer
        };
        let server_cap = capnp_rpc::new_client(Exchange(server_buffer));
        let server = connect(b, max_fds, Some(server_cap));
        let mut client = connect(a, max_fds, None);
        let exchange: exchange::Client = client.bootstrap(Side::Server);
        let work = async move {
            // Obtain the capability before the return arrives, exercising promise resolution.
            let request = exchange.get_request().send();
            let buffer = request.pipeline.get_buffer();
            let fd = capnp_rpc::get_fd(&buffer).await?;
            assert_eq!(fd.is_some(), max_fds > 0);
            assert_eq!(read_buffer(&buffer).await?, CONTENT);
            let response = request.promise.await?;
            let again = response.get()?.get_buffer()?;
            if let Some(fd) = fd {
                assert!(Rc::ptr_eq(&fd, &capnp_rpc::get_fd(&again).await?.unwrap()));
                let repeat = exchange
                    .get_request()
                    .send()
                    .promise
                    .await?
                    .get()?
                    .get_buffer()?;
                assert!(Rc::ptr_eq(&fd, &capnp_rpc::get_fd(&repeat).await?.unwrap()));
            }
            for index in 0..20 {
                let bytes = format!("client buffer {index}");
                let buffer = new_buffer(bytes.as_bytes())?;
                let mut echo = exchange.echo_request();
                echo.get().set_buffer(buffer.clone());
                let response = echo.send().promise.await?;
                let returned = response.get()?.get_buffer()?;
                assert_eq!(read_buffer(&returned).await?, bytes.as_bytes());
                assert_eq!(
                    returned.as_client_hook().get_ptr(),
                    buffer.as_client_hook().get_ptr(),
                );
            }
            Ok(())
        };
        let drivers = futures::future::try_join(client, server);
        futures::pin_mut!(work, drivers);
        match futures::future::select(work, drivers).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right((result, _)) => {
                result?;
                panic!("RPC stopped before transfer finished")
            }
        }
    }

    #[test]
    fn params_results_pipelines_and_consecutive_messages() {
        run_test(transfer(8, false));
    }

    #[test]
    fn exported_promise_resolves_with_fd() {
        run_test(transfer(8, true));
    }

    #[test]
    fn receiving_disabled_uses_rpc_fallback() {
        run_test(transfer(0, false));
    }

    #[test]
    fn rejected_promise_has_no_fd() {
        run_test(async {
            let cap: exchange::Client = capnp_rpc::new_future_client(async {
                Err(capnp::Error::failed("rejected".into()))
            });
            assert!(capnp_rpc::get_fd(&cap).await?.is_none());
            assert!(cap.get_request().send().promise.await.is_err());
            Ok(())
        });
    }

    #[test]
    fn forwarding_to_another_connection_passes_fd() {
        run_test(async {
            let (a, b) = UnixStream::pair()?;
            let server = connect(
                b,
                8,
                Some(capnp_rpc::new_client(Exchange(new_buffer(CONTENT)?))),
            );
            let mut client = connect(a, 8, None);
            let exchange: exchange::Client = client.bootstrap(Side::Server);
            let work = async move {
                let buffer = exchange
                    .get_request()
                    .send()
                    .promise
                    .await?
                    .get()?
                    .get_buffer()?;
                let (a, b) = UnixStream::pair()?;
                let forwarder = connect(b, 8, Some(capnp_rpc::new_client(Exchange(buffer))));
                let mut receiver = connect(a, 8, None);
                let exchange: exchange::Client = receiver.bootstrap(Side::Server);
                let receive = async move {
                    let buffer = exchange
                        .get_request()
                        .send()
                        .promise
                        .await?
                        .get()?
                        .get_buffer()?;
                    assert!(capnp_rpc::get_fd(&buffer).await?.is_some());
                    assert_eq!(read_buffer(&buffer).await?, CONTENT);
                    Ok(())
                };
                let drivers = futures::future::try_join(forwarder, receiver);
                futures::pin_mut!(receive, drivers);
                match futures::future::select(receive, drivers).await {
                    futures::future::Either::Left((result, _)) => result,
                    futures::future::Either::Right((result, _)) => {
                        result?;
                        panic!("forwarding stopped before transfer finished")
                    }
                }
            };
            let drivers = futures::future::try_join(client, server);
            futures::pin_mut!(work, drivers);
            match futures::future::select(work, drivers).await {
                futures::future::Either::Left((result, _)) => result,
                futures::future::Either::Right((result, _)) => {
                    result?;
                    panic!("RPC stopped before forwarding finished")
                }
            }
        });
    }
}
