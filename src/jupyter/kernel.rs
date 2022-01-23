// Copyright 2020 The Evcxr Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::api::{execute, parse_config, BoxContext};
use crate::util::*;

use crate::jupyter::connection::Connection;
use crate::jupyter::connection_file::ConnectionFile;
use crate::jupyter::jupyter_message::JupyterMessage;

use datafusion::prelude::*;

use json::JsonValue;

// Note, to avoid potential deadlocks, each thread should lock at most one mutex at a time.
#[derive(Clone)]
pub struct Server {
    iopub: Arc<Mutex<Connection>>,
    _stdin: Arc<Mutex<Connection>>,
    latest_execution_request: Arc<Mutex<Option<JupyterMessage>>>,
    shutdown_requested_receiver: Arc<Mutex<mpsc::Receiver<()>>>,
    shutdown_requested_sender: Arc<Mutex<mpsc::Sender<()>>>,
    debug: bool,
}

impl Server {
    pub fn start(connection_file: &ConnectionFile, debug: bool) -> Result<Server> {
        use zmq::SocketType;

        let zmq_context = zmq::Context::new();

        let heartbeat = bind_socket(
            connection_file,
            connection_file.hb_port,
            zmq_context.socket(SocketType::REP)?,
        )?;
        let shell_socket = bind_socket(
            connection_file,
            connection_file.shell_port,
            zmq_context.socket(SocketType::ROUTER)?,
        )?;
        let control_socket = bind_socket(
            connection_file,
            connection_file.control_port,
            zmq_context.socket(SocketType::ROUTER)?,
        )?;
        let stdin_socket = bind_socket(
            connection_file,
            connection_file.stdin_port,
            zmq_context.socket(SocketType::ROUTER)?,
        )?;
        let iopub = Arc::new(Mutex::new(bind_socket(
            connection_file,
            connection_file.iopub_port,
            zmq_context.socket(SocketType::PUB)?,
        )?));

        let (shutdown_requested_sender, shutdown_requested_receiver) = mpsc::channel();

        let server = Server {
            iopub,
            latest_execution_request: Arc::new(Mutex::new(None)),
            _stdin: Arc::new(Mutex::new(stdin_socket)),
            shutdown_requested_receiver: Arc::new(Mutex::new(shutdown_requested_receiver)),
            shutdown_requested_sender: Arc::new(Mutex::new(shutdown_requested_sender)),
            debug,
        };

        let (execution_sender, execution_receiver) = mpsc::channel();
        let (execution_response_sender, execution_response_receiver) = mpsc::channel();

        thread::spawn(move || Self::handle_hb(&heartbeat));
        server.start_thread(move |server: Server| server.handle_control(control_socket));
        server.start_thread({
            move |server: Server| {
                server.handle_shell(
                    shell_socket,
                    &execution_sender,
                    &execution_response_receiver,
                )
            }
        });

        let server_clone = server.clone();
        tokio::spawn(async move {
            server_clone
                .handle_execution_requests(execution_receiver, execution_response_sender, debug)
                .await
        });
        Ok(server)
    }

    pub(crate) fn wait_for_shutdown(&self) {
        self.shutdown_requested_receiver
            .lock()
            .unwrap()
            .recv()
            .unwrap();
    }

    fn signal_shutdown(&self) {
        self.shutdown_requested_sender
            .lock()
            .unwrap()
            .send(())
            .unwrap();
    }

    fn start_thread<F>(&self, body: F)
    where
        F: FnOnce(Server) -> Result<()> + std::marker::Send + 'static,
    {
        let server_clone = self.clone();
        thread::spawn(|| {
            if let Err(error) = body(server_clone) {
                eprintln!("{:?}", error);
            }
        });
    }

    fn handle_hb(connection: &Connection) -> Result<()> {
        let mut message = zmq::Message::new();
        let ping: &[u8] = b"ping";
        loop {
            connection.socket.recv(&mut message, 0)?;
            connection.socket.send(ping, 0)?;
        }
    }

    async fn handle_execution_requests(
        self,
        execution_receiver: mpsc::Receiver<JupyterMessage>,
        execution_response_sender: mpsc::Sender<JupyterMessage>,
        _: bool,
    ) -> Result<()> {
        let mut execution_count: i32 = 0;
        let config = ExecutionConfig::new().with_batch_size(32768);
        let mut execution_ctx = ExecutionContext::with_config(config);
        let box_ctx = BoxContext::new(None, None);

        loop {
            let message = execution_receiver.recv()?;

            // If we want this clone to be cheaper, we probably only need the header, not the
            // whole message.
            *self.latest_execution_request.lock().unwrap() = Some(message.clone());
            let src = message.code();
            execution_count += 1;

            message
                .new_message("execute_input")
                .with_content(object! {
                    "execution_count" => execution_count,
                    "code" => src
                })
                .send(&*self.iopub.lock().unwrap())?;

            // replace
            let src = variables::replace_hocon_parameters(src);

            match parse_config(box_ctx.clone(), format!("[{}]", src).as_str(), true, false) {
                Ok(stages) => {
                    match execute(box_ctx.clone(), &mut execution_ctx, stages, false).await {
                        Ok(result) => {
                            let html = create_html_table(
                                result.unwrap().collect().await.unwrap(),
                                Some(10),
                                &["tex2jax_ignore"],
                            )
                            .unwrap();

                            let mut data: HashMap<String, JsonValue> = HashMap::new();
                            data.insert("text/html".into(), json::from(html));
                            message
                                .new_message("execute_result")
                                .with_content(object! {
                                    "execution_count" => execution_count,
                                    "data" => data,
                                    "metadata" => object!(),
                                })
                                .send(&*self.iopub.lock().unwrap())?;

                            execution_response_sender.send(message.new_reply().with_content(
                                object! {
                                    "status" => "ok",
                                    "execution_count" => execution_count,
                                },
                            ))?;
                        }
                        Err(e) => {
                            message
                                .new_message("error")
                                .with_content(object! {
                                    "ename": "Error",
                                    "evalue": e.to_string(),
                                    "traceback" => array![
                                        e.to_string()
                                    ],
                                })
                                .send(&*self.iopub.lock().unwrap())?;
                            execution_response_sender.send(message.new_reply().with_content(
                                object! {
                                    "status" => "error",
                                    "execution_count" => execution_count
                                },
                            ))?;
                        }
                    }
                }
                Err(e) => {
                    message
                        .new_message("error")
                        .with_content(object! {
                            "ename": "Error",
                                    "evalue": e.to_string(),
                                    "traceback" => array![
                                e.to_string()
                            ],
                        })
                        .send(&*self.iopub.lock().unwrap())?;
                    execution_response_sender.send(message.new_reply().with_content(object! {
                        "status" => "error",
                        "execution_count" => execution_count
                    }))?;
                }
            };
        }
    }

    fn handle_shell(
        self,
        connection: Connection,
        execution_sender: &mpsc::Sender<JupyterMessage>,
        execution_response_receiver: &mpsc::Receiver<JupyterMessage>,
    ) -> Result<()> {
        loop {
            let message = JupyterMessage::read(&connection, self.debug)?;
            self.handle_shell_message(
                message,
                &connection,
                execution_sender,
                execution_response_receiver,
            )?;
        }
    }

    fn handle_shell_message(
        &self,
        message: JupyterMessage,
        connection: &Connection,
        execution_sender: &mpsc::Sender<JupyterMessage>,
        execution_response_receiver: &mpsc::Receiver<JupyterMessage>,
    ) -> Result<()> {
        // Processing of every message should be enclosed between "busy" and "idle"
        // see https://jupyter-client.readthedocs.io/en/latest/messaging.html#messages-on-the-shell-router-dealer-channel
        // Jupiter Lab doesn't use the kernel until it received "idle" for kernel_info_request
        message
            .new_message("status")
            .with_content(object! {"execution_state" => "busy"})
            .send(&*self.iopub.lock().unwrap())?;

        let idle = message
            .new_message("status")
            .with_content(object! {"execution_state" => "idle"});

        if message.message_type() == "kernel_info_request" {
            message
                .new_reply()
                .with_content(kernel_info())
                .send(connection)?;
        } else if message.message_type() == "is_complete_request" {
            message
                .new_reply()
                .with_content(object! {"status" => "complete"})
                .send(connection)?;
        } else if message.message_type() == "execute_request" {
            execution_sender.send(message)?;
            execution_response_receiver.recv()?.send(connection)?;
        } else if message.message_type() == "comm_msg"
            || message.message_type() == "comm_info_request"
        {
            // We don't handle this yet.
            // } else if message.message_type() == "complete_request" {
            //     let reply = message.new_reply().with_content(
            //         match handle_completion_request(&context, message) {
            //             Ok(response_content) => response_content,
            //             Err(error) => object! {
            //                 "status" => "error",
            //                 "ename" => error.to_string(),
            //                 "evalue" => "",
            //             },
            //         },
            //     );
            //     reply.send(&connection)?;
        } else {
            eprintln!(
                "Got unrecognized message type on shell channel: {}",
                message.message_type()
            );
        }
        idle.send(&*self.iopub.lock().unwrap())?;
        Ok(())
    }

    fn handle_control(self, connection: Connection) -> Result<()> {
        loop {
            let message = JupyterMessage::read(&connection, self.debug)?;
            match message.message_type() {
                "shutdown_request" => self.signal_shutdown(),
                "interrupt_request" => {
                    message.new_reply().send(&connection)?;
                    eprintln!(
                        "Rust doesn't support interrupting execution. Perhaps restart kernel?"
                    );
                }
                _ => {
                    eprintln!(
                        "Got unrecognized message type on control channel: {}",
                        message.message_type()
                    );
                }
            }
        }
    }
}

fn bind_socket(
    connection_file: &ConnectionFile,
    port: u16,
    socket: zmq::Socket,
) -> Result<Connection> {
    let endpoint = format!(
        "{}://{}:{}",
        connection_file.transport, connection_file.ip, port
    );
    socket.bind(&endpoint)?;
    Connection::new(socket, &connection_file.key)
}

/// See [Kernel info documentation](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info)
fn kernel_info() -> JsonValue {
    object! {
        "protocol_version" => "5.3",
        "implementation" => env!("CARGO_PKG_NAME"),
        "implementation_version" => env!("CARGO_PKG_VERSION"),
        "language_info" => object!{
            "name" => "javascript",
            "version" => env!("CARGO_PKG_VERSION"),
            "mimetype" => "application/json",
            "pygment_lexer" => "javascript",
            "codemirror_mode" => object!{
                "name" => "javascript",
                "statementIndent" => 2,
                "tabSize" => 2,
                "smartIndent" => false,
            }
        },
        "banner" => format!("Box {}", env!("CARGO_PKG_VERSION")),
        "help_links" => array![],
        "status" => "ok"
    }
}
