use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{OptionExt, ResultExt, Snafu, Whatever};
use tokio::{
    fs::File,
    io::AsyncWriteExt as _,
    sync::{Mutex, RwLock},
};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, trace};

#[derive(Debug, Snafu)]
pub enum Error {
    Serialize {
        source: serde_json::Error,
    },
    Io {
        cause: String,
        source: tokio::io::Error,
    },
    NotFound,
}

#[derive(Serialize)]
pub struct LogRecord<'k, 'v> {
    #[serde(borrow)]
    key: &'k str,
    #[serde(borrow)]
    value: &'v Value,
}

pub struct DataStore {
    write_log: Mutex<File>,
    in_memory: RwLock<BTreeMap<String, Value>>,
}

impl DataStore {
    pub async fn new(log_path: impl AsRef<std::path::Path>) -> Result<Self, Whatever> {
        Ok(Self {
            write_log: Mutex::new(
                File::options()
                    .append(true)
                    .create(true)
                    .write(true)
                    .open(log_path)
                    .await
                    .whatever_context("open write log file")?,
            ),
            in_memory: RwLock::default(),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Value, Error> {
        debug!(key, "get");
        self.in_memory
            .read()
            .await
            .get(key)
            .inspect(|value| trace!(%value, "got"))
            .context(NotFoundSnafu)
            .cloned()
    }

    pub async fn put(&self, key: String, value: Value) -> Result<(), Error> {
        debug!(key, %value, "put");
        let record = LogRecord {
            key: &key,
            value: &value,
        };
        let mut record_bytes = serde_json::to_vec(&record).context(SerializeSnafu)?;
        record_bytes.push(b'\n');
        trace!("writing to log");
        self.write_log
            .lock()
            .await
            .write_all(&record_bytes)
            .await
            .context(IoSnafu {
                cause: "write to write log",
            })?;

        trace!("inserting value into in-memory store");
        self.in_memory.write().await.insert(key, value);

        Ok(())
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND.into_response(),
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

#[tracing::instrument(skip(data_store))]
async fn get_value(
    State(data_store): State<Arc<DataStore>>,
    Path(key): Path<String>,
) -> Result<Json<Value>, Error> {
    data_store.get(&key).await.map(Json)
}

#[tracing::instrument(skip(value, data_store))]
async fn put_value(
    State(data_store): State<Arc<DataStore>>,
    Path(key): Path<String>,
    Json(value): Json<Value>,
) -> Result<(), Error> {
    data_store.put(key, value).await
}

fn data_router() -> Router<Arc<DataStore>> {
    Router::new()
        .route("/{key}", get(get_value))
        .route("/{key}", put(put_value))
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<(), Whatever> {
    tracing_subscriber::fmt::init();
    info!("Hello, world!");

    let data_store = Arc::new(
        DataStore::new("/tmp/write-log")
            .await
            .whatever_context("initalize data store")?,
    );

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .nest("/d", data_router())
        .with_state(data_store)
        .layer(TraceLayer::new_for_http());

    let addr = "0.0.0.0:3333";
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .whatever_context("create TCP listener")?;

    info!(listening_on = addr);
    axum::serve(listener, app)
        .await
        .whatever_context("serve app")
}
