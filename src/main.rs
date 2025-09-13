use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use prometheus_client::{encoding::text::encode, metrics::gauge::Gauge};
use std::sync::atomic::AtomicU32;
use thiserror::Error;
use tokio::fs::read_dir;
use tokio::time::{Duration, sleep};

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Error, Debug)]
enum SPError {
    #[error("Error: {0}")]
    Other(String),
}

#[derive(Default, Debug)]
pub struct AppState {
    pub registry: Registry,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct InstanceLabels {
    pub topic: String,
}

#[derive(Debug)]
pub struct Metrics {
    temperature: Family<InstanceLabels, Gauge<f32, AtomicU32>>,
}

async fn get_w1path(dir: &Path) -> Result<String, SPError> {
    let mut entries = match read_dir(dir).await {
        Ok(entries) => entries,
        Err(_) => {
            return Err(SPError::Other(format!(
                "Can't find directory: {}",
                dir.to_path_buf().to_str().unwrap()
            )));
        }
    };

    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.file_name().to_str().unwrap().starts_with("28") {
            return Ok(entry.file_name().into_string().unwrap());
        }
    }
    Err(SPError::Other("Can't find correct file".to_owned()))
}
async fn read_temperature(
    inst: InstanceLabels,
    metrics: Arc<Mutex<Metrics>>,
) -> Result<bool, SPError> {
    let w1_path = Path::new("/sys/bus/w1/devices/");
    let dir = match get_w1path(w1_path).await {
        Ok(dir) => dir,
        Err(error) => return std::result::Result::Err(error),
    };

    let w1_slave: PathBuf = [w1_path, Path::new(&dir), "w1_slave".as_ref()]
        .iter()
        .collect();

    let data = fs::read_to_string(w1_slave).unwrap();
    let (_, number) = data.split_at(data.find("t=").unwrap() + 2);
    let temp = number.trim().parse::<f32>().unwrap() / 1000.0;

    metrics
        .lock()
        .await
        .temperature
        .get_or_create(&inst)
        .set(temp);
    Ok(true)
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    let program = args.next().unwrap(); // skip program name

    let label = args.next().unwrap_or_else(|| {
        eprintln!("Usage: {} <label> [port]", program);
        std::process::exit(1);
    });

    let port: u16 = args.next().and_then(|p| p.parse().ok()).unwrap_or(9090);

    let metrics = Metrics {
        temperature: Family::default(),
    };
    let mut state = AppState {
        registry: Registry::default(),
    };
    state.registry.register(
        "temperature",
        "the temperature",
        metrics.temperature.clone(),
    );

    let state = Arc::new(Mutex::new(state));
    let metrics = Arc::new(Mutex::new(metrics));
    let inst = InstanceLabels { topic: label };

    tokio::spawn(async move {
        loop {
            if let Err(error) = read_temperature(inst.to_owned(), metrics.clone()).await {
                println!("{}", error);
            }
            sleep(Duration::from_millis(1000)).await;
        }
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/metrics", get(metricspage).with_state(state));
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Yo"
}

async fn metricspage(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().await;
    let mut buffer = String::new();
    encode(&mut buffer, &state.registry).unwrap();

    Response::builder()
        .status(StatusCode::OK)
        .header(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(buffer))
        .unwrap()
}
