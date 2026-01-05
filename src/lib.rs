//! Runtime-agnostic async wrapper for [`ssh2`].

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]

mod agent;
mod channel;
mod runtime;
mod session;
mod sftp;
