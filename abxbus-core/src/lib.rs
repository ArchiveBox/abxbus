//! Canonical abxbus core.
//!
//! This crate is intentionally not a native-language runtime. Hosts execute
//! user callbacks; this core owns records, default resolution, scheduler
//! eligibility, locks, timeout/cancellation state, and flat wire envelopes.

pub mod core;
pub mod defaults;
pub mod envelope;
pub mod id;
pub mod locks;
pub mod protocol;
pub mod scheduler;
pub mod store;
pub mod time;
pub mod types;

pub use core::*;
pub use defaults::*;
pub use envelope::*;
pub use id::*;
pub use locks::*;
pub use protocol::*;
pub use scheduler::*;
pub use store::*;
pub use time::*;
pub use types::*;
