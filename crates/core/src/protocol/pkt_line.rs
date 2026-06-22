use crate::error::CloneError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Data(Vec<u8>),
    Flush,
    Delimiter,
    ResponseEnd,
}

pub fn encode_data(line: &str, out: &mut Vec<u8>) {
    let len = line.len() + 4;
    out.extend_from_slice(format!("{len:04x}").as_bytes());
    out.extend_from_slice(line.as_bytes());
}

pub fn encode_flush(out: &mut Vec<u8>) {
    out.extend_from_slice(b"0000");
}

pub fn encode_delimiter(out: &mut Vec<u8>) {
    out.extend_from_slice(b"0001");
}

pub fn parse_packets(
    url: &str,
    operation: &'static str,
    bytes: &[u8],
) -> Result<Vec<Packet>, CloneError> {
    let mut packets = Vec::new();
    let mut cursor = 0usize;

    while cursor < bytes.len() {
        if bytes.len() - cursor < 4 {
            return Err(CloneError::MalformedRemoteResponse {
                url: url.to_owned(),
                operation,
                detail: "pkt-line header was truncated".to_owned(),
            });
        }

        let header = std::str::from_utf8(&bytes[cursor..cursor + 4]).map_err(|error| {
            CloneError::MalformedRemoteResponse {
                url: url.to_owned(),
                operation,
                detail: format!("pkt-line header was not UTF-8 hex: {error}"),
            }
        })?;
        cursor += 4;

        match header {
            "0000" => packets.push(Packet::Flush),
            "0001" => packets.push(Packet::Delimiter),
            "0002" => packets.push(Packet::ResponseEnd),
            _ => {
                let len = usize::from_str_radix(header, 16).map_err(|error| {
                    CloneError::MalformedRemoteResponse {
                        url: url.to_owned(),
                        operation,
                        detail: format!("pkt-line length `{header}` was invalid: {error}"),
                    }
                })?;
                if len < 4 {
                    return Err(CloneError::MalformedRemoteResponse {
                        url: url.to_owned(),
                        operation,
                        detail: format!("pkt-line length `{len}` is smaller than its header"),
                    });
                }
                let data_len = len - 4;
                if bytes.len() - cursor < data_len {
                    return Err(CloneError::MalformedRemoteResponse {
                        url: url.to_owned(),
                        operation,
                        detail: "pkt-line payload was truncated".to_owned(),
                    });
                }
                packets.push(Packet::Data(bytes[cursor..cursor + data_len].to_vec()));
                cursor += data_len;
            }
        }
    }

    Ok(packets)
}
