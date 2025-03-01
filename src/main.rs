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
use snafu::{ResultExt, Snafu, Whatever, ensure};
use std::{
    path::PathBuf,
    sync::{Arc, LazyLock},
};
use tower_http::trace::TraceLayer;
use tracing::{error, info};

// TODO: merge sstables

fn default_max_in_memory_values() -> usize {
    1024
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub listen_addr: Option<String>,
    pub storage_dir: PathBuf,
    #[serde(default = "default_max_in_memory_values")]
    pub max_in_memory_values: usize,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    envy::prefixed("DAT_")
        .from_env()
        .expect("load configuration")
});

#[derive(Debug, Snafu)]
pub enum Error {
    Serialize {
        source: postcard::Error,
    },
    Io {
        cause: String,
        source: tokio::io::Error,
    },
    NotFound,
    InvalidValue,
    FileFormatVersion,
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

    let data_store = Arc::new(
        DataStore::new(&CONFIG.storage_dir)
            .await
            .whatever_context("initalize data store")?,
    );

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .nest("/d", data_router())
        .with_state(data_store)
        .layer(TraceLayer::new_for_http());

    let addr = CONFIG.listen_addr.as_deref().unwrap_or("0.0.0.0:3333");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .whatever_context("create TCP listener")?;

    info!(listening_on = addr);
    axum::serve(listener, app)
        .await
        .whatever_context("serve app")
}
