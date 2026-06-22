mod pkt_line;
mod smart_http;

pub use smart_http::{
    Remote, RemoteRef, discover_remote, fetch_full_pack, fetch_full_pack_pipelined, http_client,
};
