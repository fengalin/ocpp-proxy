use anyhow::Context;
use clap::Parser;
use futures::{future::FusedFuture, pin_mut, prelude::*};
use log::*;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    ops::ControlFlow,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite as ts};

const PROXY_PORT: u16 = 9000;

#[derive(Debug, clap::Parser)]
#[command(version, about = "Runs an OCPP proxy")]
pub struct Args {
    #[clap(long, help = "server IP address", default_value = "192.168.1.49")]
    pub server_ip: String,
}

async fn start(
    cp_ws_stream: TcpStream,
    server_address: &str,
    mut ctrl_c: std::pin::Pin<&mut impl FusedFuture<Output = Result<(), std::io::Error>>>,
) -> anyhow::Result<ControlFlow<()>> {
    let mut cp_ws_stream = accept_async(cp_ws_stream).await?;

    let (mut server_ws_stream, server_response) = connect_async(server_address).await?;
    info!("Connected to: {server_address}");
    info!("Response HTTP cpde: {}", server_response.status());
    for (header, _value) in server_response.headers() {
        info!("* {header}");
    }

    let mut cp_message = None;
    let mut server_message = None;

    let mut prev_ctl_flow = ControlFlow::Continue::<()>(());
    loop {
        // FIXME add timeout to exit after a ws close was detected
        futures::select_biased! {
            _ = ctrl_c => {
                warn!("shutting down due to SIGINT");
                match server_ws_stream.close(None).await.context("closing server websocket") {
                    Ok(_) => {
                        let recv_res = server_ws_stream.next().await;
                        info!("server replied {recv_res:?}");
                    }
                    Err(err) => error!("{err}"),
                }
                cp_ws_stream.close(None).await.context("closing charging point websocket")?;
                let recv_res = cp_ws_stream.next().await;
                info!("charging point replied {recv_res:?}");
                return Ok(ControlFlow::Break(()));
            }
            message = cp_ws_stream.next() => cp_message = message,
            message = server_ws_stream.next() => server_message = message,
        };

        let mut cp_ctrl_flow = ControlFlow::Continue(());
        if let Some(cp_msg) = cp_message.take() {
            let cp_msg = cp_msg.inspect_err(|err| match err {
                ts::Error::ConnectionClosed | ts::Error::Protocol(_) | ts::Error::Utf8(_) => (),
                other => error!("Error processing cp message: {other}"),
            })?;

            cp_ctrl_flow = handle_incoming_ws_message(cp_msg, "cp ", &mut server_ws_stream)
                .await
                .context("fowarding cp message")?;
        };

        let mut server_ctrl_flow = ControlFlow::Continue(());
        if let Some(server_msg) = server_message.take() {
            let server_msg = server_msg.inspect_err(|err| match err {
                ts::Error::ConnectionClosed | ts::Error::Protocol(_) | ts::Error::Utf8(_) => (),
                other => error!("Error processing server message: {other}"),
            })?;

            server_ctrl_flow = handle_incoming_ws_message(server_msg, "srv", &mut cp_ws_stream)
                .await
                .context("fowarding server message")?;
        };

        if prev_ctl_flow.is_break() {
            warn!("exiting due to previous Close message");
            break;
        }

        if cp_ctrl_flow.is_break() && server_ctrl_flow.is_break() {
            warn!("exiting due Close handshake");
            break;
        }

        if cp_ctrl_flow.is_break() || server_ctrl_flow.is_break() {
            prev_ctl_flow = ControlFlow::Break(());
        }
    }

    Ok(ControlFlow::Continue(()))
}

async fn handle_incoming_ws_message<D: Sink<ts::Message> + Unpin>(
    msg: ts::Message,
    origin: &str,
    dest: &mut D,
) -> Result<ControlFlow<()>, D::Error> {
    let mut ret = ControlFlow::Continue(());

    match &msg {
        ts::Message::Text(text) => {
            info!("{origin}: msg text: {text}");
        }
        ts::Message::Binary(binary) => {
            info!("{origin}: msg bin: {binary:?}");
        }
        ts::Message::Ping(_) => {
            info!("{origin}: ping");
        }
        ts::Message::Pong(_) => {
            info!("{origin}: pong");
        }
        ts::Message::Close(payload) => {
            info!("{origin}: websocket closed {payload:?}");
            ret = ControlFlow::Break(());
        }
        other => {
            info!("{origin}: other message: {other:?}");
        }
    }

    dest.send(msg).await?;

    Ok(ret)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut env_logger_builder = env_logger::builder();

    #[cfg(debug_assertions)]
    env_logger_builder
        .format_source_path(true)
        .format_line_number(true);

    env_logger_builder
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .try_init()
        .unwrap();

    // rustls::crypto::aws_lc_rs::default_provider()
    //     .install_default()
    //     .unwrap();

    let server_ip = args.server_ip.parse::<Ipv4Addr>().context("server IP")?;
    let server_address = format!("ws://{server_ip}:9000");

    let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, PROXY_PORT);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bindind to {addr}"))?;

    loop {
        let ctrl_c = tokio::signal::ctrl_c().fuse();
        pin_mut!(ctrl_c);

        info!("Listening on: {addr}");
        let accept_stream = listener.accept().fuse();
        pin_mut!(accept_stream);

        futures::select_biased! {
            _ = ctrl_c => {
                warn!("shutting down due to SIGINT");
                break;
            }
            accept_res = accept_stream => {
                let Ok((ws_stream, _)) = accept_res else {
                    warn!("TCP listener terminated");
                    continue;
                };
                let peer = ws_stream.peer_addr().context("getting peer address")?;
                info!("peer address {peer}");

                match start(ws_stream, &server_address, ctrl_c.as_mut()).await {
                    Ok(ret) if ret.is_break() => break,
                    Ok(_) => (),
                    Err(err) =>
                        error!("terminating connection: {err:?}"),
                }
            }
        }
    }

    Ok(())
}
