pub mod backend;
mod client;

pub use backend::Backend as SinkBackend;
pub use client::Error;
pub use client::SinkClient;
