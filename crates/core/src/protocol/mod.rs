mod pkt_line;
mod smart_http;

pub use smart_http::{Remote, RemoteRef, discover_remote, fetch_full_pack, http_client};
