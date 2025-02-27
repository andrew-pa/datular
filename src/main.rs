use std::sync::Arc;

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use snafu::{ResultExt, Snafu, Whatever};
use tower_http::trace::TraceLayer;
use tracing::{error, info};

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

pub mod sstable;
pub mod store;
pub mod wal;

use store::DataStore;

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

fn data_router() -> Router<Arc<DataStore>> {
    Router::new()
        .route(
            "/{key}",
            get(
                async |State(data_store): State<Arc<DataStore>>, Path(key): Path<String>| {
                    data_store.get(&key).await
                },
            ),
        )
        .route(
            "/{key}",
            put(
                async |State(data_store): State<Arc<DataStore>>,
                       Path(key): Path<String>,
                       value: Bytes| { data_store.put(key, value).await },
            ),
        )
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<(), Whatever> {
    tracing_subscriber::fmt::init();
    info!("Hello, world!");

    let data_store = Arc::new(
        DataStore::new("/tmp/datular".into())
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
