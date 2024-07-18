#![doc(html_root_url = "https://docs.rs/faasrail-loadgen/0.0.1")]
#![deny(
    //missing_docs, // TODO
    unreachable_pub,
    //rustdoc::all,
)]

pub mod fixer;
pub mod sink;
pub mod source;
mod wreq;

pub use wreq::WorkloadRequest;

/// Simple type alias for [`CompactString`], to represent each invocation's ID.
///
/// [`CompactString`]: ::compact_str::CompactString
pub type InvocationId = ::compact_str::CompactString;
