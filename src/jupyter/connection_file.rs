use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct ConnectionFile {
    pub control_port: u16,
    pub shell_port: u16,
    pub transport: String,
    pub signature_scheme: String,
    pub stdin_port: u16,
    pub hb_port: u16,
    pub ip: String,
    pub iopub_port: u16,
    pub key: String,
}
