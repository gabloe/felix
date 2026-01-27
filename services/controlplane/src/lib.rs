//! Control-plane service library crate.
//!
//! # Purpose
//! Exposes the control-plane API surface, auth helpers, configuration, and
//! storage implementations for use by the binary and tests.
//!
//! # Notes
//! Module boundaries mirror the HTTP API and storage backends for clarity.
pub mod api;
pub mod app;
pub mod auth;
pub mod config;
pub mod model;
pub mod observability;
pub mod store;
