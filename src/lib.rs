//! Runtime-agnostic async wrapper for [`ssh2`].

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]

mod agent;
mod channel;
mod consts;
pub mod runtime;
mod session;
mod sftp;

pub use ssh2;
