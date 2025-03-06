pub mod kv;
pub mod sstable;
pub mod store;
pub mod wal;
use store::DataStore;

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{MethodRouter, get},
};
use serde::Deserialize;
use snafu::{ResultExt, Snafu, Whatever, ensure, ensure_whatever};
use std::{
    path::PathBuf,
    sync::{Arc, LazyLock},
};
use tower_http::trace::TraceLayer;
use tracing::{error, info};

// TODO: sstable index could be its own thing

fn default_max_in_memory_values() -> usize {
    1024
}

fn default_max_in_memory_bytes() -> usize {
    // 16 MiB
    16 * 1024 * 1024
}

fn default_num_sstables_to_merge() -> usize {
    4
}

#[derive(Debug, Deserialize)]
pub struct Config {
    /// The address that the HTTP server will listen on.
    pub listen_addr: Option<String>,
    /// The directory to write stored data to.
    pub storage_dir: PathBuf,
    /// The maximum number of key-value pairs to be held in memory before flushing.
    #[serde(default = "default_max_in_memory_values")]
    pub max_in_memory_values: usize,
    /// The maximum number of bytes worth of key-value pairs to be held in memory before flushing.
    #[serde(default = "default_max_in_memory_bytes")]
    pub max_in_memory_bytes: usize,
    /// The number of SSTables to merge together in one merge operation, moving them to the next level.
    #[serde(default = "default_num_sstables_to_merge")]
    pub num_sstables_to_merge: usize,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    envy::prefixed("DAT_")
        .from_env()
        .expect("load configuration")
});

#[derive(Debug, Snafu)]
pub enum Error {
    Serialize {
        source: bincode::Error,
    },
    #[snafu(display("I/O error: {cause}"))]
    Io {
        cause: String,
        source: tokio::io::Error,
    },
    NotFound,
    InvalidValue,
    FileFormatVersion,
    #[snafu(display("Retried too many times: {cause}"))]
    TooManyRetries { cause: String }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND.into_response(),
            Self::InvalidValue => StatusCode::BAD_REQUEST.into_response(),
            e => {
                error!(
                    "error occurred handling request: {}",
                    snafu::Report::from_error(e)
                );
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

fn data_router() -> Router<Arc<DataStore>> {
    Router::new().route(
        "/{key}",
        MethodRouter::new()
            .get(
                async |State(data_store): State<Arc<DataStore>>, Path(key): Path<String>| {
                    data_store.get(&key).await
                },
            )
            .put(
                async |State(data_store): State<Arc<DataStore>>,
                       Path(key): Path<String>,
                       value: Bytes| {
                    ensure!(!value.is_empty(), InvalidValueSnafu);
                    data_store.put(key, value).await
                },
            )
            .delete(
                async |State(data_store): State<Arc<DataStore>>, Path(key): Path<String>| {
                    data_store.put(key, Bytes::new()).await
                },
            ),
    )
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<(), Whatever> {
    tracing_subscriber::fmt::init();
    info!("Hello, world!");

    ensure_whatever!(
        CONFIG.max_in_memory_values > 0,
        "Max in-memory values must be more than zero"
    );
    ensure_whatever!(
        CONFIG.num_sstables_to_merge > 1,
        "Number of SSTables to merge must be more than one"
    );

    let data_store = DataStore::new(&CONFIG.storage_dir)
        .await
        .whatever_context("initalize data store")?;

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .nest("/d", data_router())
        .with_state(data_store)
        .layer(TraceLayer::new_for_http());

    let addr = CONFIG.listen_addr.as_deref().unwrap_or("0.0.0.0:3333");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_whatever_context(|_| format!("create TCP listener for address {addr}"))?;

    info!(listening_on = addr);
    axum::serve(listener, app)
        .await
        .whatever_context("serve app")
}
