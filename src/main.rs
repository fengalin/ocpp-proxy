use anyhow::Context;
use clap::Parser;
use futures::prelude::*;
use log::*;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    ops::ControlFlow,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::{accept_async, connect_async, tungstenite as ts};

const PROXY_PORT: u16 = 9000;

#[derive(Debug, clap::Parser)]
#[command(version, about = "Runs an OCPP proxy")]
pub struct Args {
    #[clap(long, help = "server IP address", default_value = "192.168.1.49")]
    pub server_ip: String,
}

async fn run(
    mut stop_rx: mpsc::Receiver<()>,
    listener: TcpListener,
    addr: SocketAddrV4,
    server_address: String,
) -> anyhow::Result<()> {
    loop {
        info!("Listening on: {addr}");

        let (cp_ws_stream, cp_addr) = tokio::select! {
            biased;
            _ = stop_rx.recv() => {
                info!("terminating due to stop cmd");
                break;
            }
            cp_accept_res = listener.accept() => {
                cp_accept_res.context("listening to CP")?
            }
        };

        info!("peer address {cp_addr}");

        let mut cp_ws_stream = tokio::select! {
            biased;
            _ = stop_rx.recv() => {
                info!("terminating due to stop cmd");
                return Ok(());
            }
            cp_ws_accept_res = accept_async(cp_ws_stream) => {
                match cp_ws_accept_res {
                    Ok(cp_ws_stream) => cp_ws_stream,
                    Err(err) => {
                        error!("accepting CP WS stream: {err}");
                        continue;
                    }
                }
            }
        };

        let (mut server_ws_stream, server_response) = loop {
            tokio::select! {
                biased;
                _ = stop_rx.recv() => {
                    info!("terminating due to stop cmd");
                    let _ = cp_ws_stream.close(None).await;
                    return Ok(());
                }
                server_connect_res = connect_async(&server_address) => {
                    match server_connect_res {
                        Ok(server_connect_ret) => break server_connect_ret,
                        Err(err) => {
                            error!("conecting to server: {err}");
                            let _ = cp_ws_stream.close(None).await;
                            // FIXME might want to give up after a few tries?
                            continue;
                        }
                    }
                }
            }
        };

        info!("Connected to: {server_address}");
        info!("Response HTTP code: {}", server_response.status());
        for (header, _value) in server_response.headers() {
            info!("* {header}");
        }

        let mut cp_message = None;
        let mut server_message = None;

        loop {
            tokio::select! {
                biased;
                _ = stop_rx.recv() => {
                    info!("terminating due to stop cmd");
                    let _ = server_ws_stream.close(None).await;
                    let _ = cp_ws_stream.close(None).await;
                    return Ok(());
                }
                message = cp_ws_stream.next() => cp_message = message,
                message = server_ws_stream.next() => server_message = message,
            }

            if let Some(cp_msg) = cp_message.take() {
                let cp_msg = match cp_msg {
                    Ok(cp_msg) => cp_msg,
                    Err(err) => {
                        error!("processing cp message: {err}");
                        let _ = server_ws_stream.close(None).await;
                        break;
                    }
                };

                match handle_incoming_ws_message(cp_msg, "cp ", &mut server_ws_stream).await {
                    Ok(ctrl_flow) if ctrl_flow.is_break() => {
                        warn!("CP ws terminated");
                        let _ = server_ws_stream.close(None).await;
                        break;
                    }
                    Err(err) => {
                        error!("handling message from CP: {err}");
                        // FIXME probably permanent
                        continue;
                    }
                    _ => (),
                }
            }

            if let Some(server_msg) = server_message.take() {
                let server_msg = match server_msg {
                    Ok(server_msg) => server_msg,
                    Err(err) => {
                        error!("processing server message: {err}");
                        let _ = cp_ws_stream.close(None).await;
                        break;
                    }
                };

                match handle_incoming_ws_message(server_msg, "srv", &mut cp_ws_stream).await {
                    Ok(ctrl_flow) if ctrl_flow.is_break() => {
                        warn!("Server ws terminated");
                        let _ = cp_ws_stream.close(None).await;
                        break;
                    }
                    Err(err) => {
                        error!("handling message from Server: {err}");
                        // FIXME probably permanent
                        continue;
                    }
                    _ => (),
                }
            }
        }
    }

    Ok(())
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

    let (stop_tx, stop_rx) = mpsc::channel(8);

    let run_hdl = tokio::spawn(run(stop_rx, listener, addr, server_address));

    let _ = tokio::signal::ctrl_c().await;
    warn!("shutting down due to SIGINT");

    let _ = stop_tx.send(()).await;
    if let Ok(Err(err)) = run_hdl.await {
        error!("proxy terminated: {err}");
    }

    Ok(())
}
