use std::io::ErrorKind;

use axum::body::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Error};

pub async fn write_key_value(
    f: &mut (impl AsyncWrite + Unpin),
    key: &str,
    value: &Bytes,
) -> Result<(), Error> {
    f.write_u32_le(key.len() as u32).await?;
    f.write_u32_le(value.len() as u32).await?;
    f.write_all(key.as_bytes()).await?;
    f.write_all(value).await
}

pub async fn read_next_key_value(
    f: &mut (impl AsyncRead + Unpin),
) -> Result<Option<(String, Bytes)>, Error> {
    let key_len = match f.read_u32_le().await {
        Ok(len) => len as usize,
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    let val_len = f.read_u32_le().await? as usize;
    let mut buf: Vec<u8> = vec![0; key_len + val_len];
    f.read_exact(&mut buf).await?;
    let value = buf.split_off(key_len);

    let key = unsafe { String::from_utf8_unchecked(buf) };

    Ok(Some((key, value.into())))
}
