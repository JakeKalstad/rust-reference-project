use futures::stream::SplitSink;
use futures::SinkExt;
use futures::StreamExt;
use handlebars::Handlebars;
use lazy_static::lazy_static;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use taos::*;
use tokio::{task::spawn, time::interval};
use tokio_cron_scheduler::{Job, JobScheduler};

use warp::Filter;

// Websocket (WS) stuff (it's a lto)
// WS example request structure
#[derive(Deserialize, Debug)]
struct WsRequest {
    ts: String,
    ttype: String,
    uuid: String,
    ticker: String,
    price: f64,
    qty: i32,
}
// implement a method for the WsRequest struct
impl WsRequest {
    fn insert(&self) -> String {
        return format!(
            "INSERT INTO {DB} VALUES ('{}', '{}', '{}', '{}', {}, {})",
            self.ts, self.uuid, self.ttype, self.ticker, self.price, self.qty,
        );
    }
}

// example WS result structure
#[derive(Serialize, Debug)]
struct WsResult {
    status: i8,
}

// arbitrary codes
const SUCCESS: i8 = 1;
const INPUT_ERROR: i8 = 2;
const SOCKET_ERROR: i8 = 3;
const INSERT_ERROR: i8 = 4;

// client func to split the socket into receive/send and slurp messages into a handler
async fn handle_ws_client(websocket: warp::ws::WebSocket) {
    let (mut sender, mut receiver) = websocket.split();
    while let Some(body) = receiver.next().await {
        let message = match body {
            Ok(msg) => msg,
            Err(e) => {
                log::error!("{}", e);
                send_error(&mut sender, SOCKET_ERROR).await;
                break;
            }
        };
        handle_websocket(message, &mut sender).await;
    }
    log::info!("Client Disconnected")
}

// handler func for each individual WS message coming in
async fn handle_websocket(
    message: warp::ws::Message,
    sender: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
) {
    let msg = if let Ok(s) = message.to_str() {
        s
    } else {
        log::error!("No parseable input");
        send_error(sender, INPUT_ERROR).await;
        return;
    };
    let get_req = serde_json::from_str(msg);
    let req: WsRequest = match get_req {
        Ok(ws) => ws,
        Err(error) => {
            log::error!("{}", error);
            send_error(sender, INPUT_ERROR).await;
            return;
        }
    };
    match TAOS.exec(req.insert()).await {
        Ok(_) => send_success(sender).await,
        Err(error) => {
            log::error!("{}", error);
            send_error(sender, INSERT_ERROR).await;
            return;
        }
    };
}

async fn send_error(sender: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>, status: i8) {
    let response = match serde_json::to_string(&WsResult { status: status }) {
        Ok(ws) => ws,
        Err(error) => {
            log::error!("{}", error);
            return;
        }
    };
    match sender.send(warp::ws::Message::text(response)).await {
        Ok(()) => (),
        Err(error) => log::error!("{}", error),
    }
}

async fn send_success(sender: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>) {
    let response = match serde_json::to_string(&WsResult { status: SUCCESS }) {
        Ok(r) => r,
        Err(error) => {
            log::error!("{}", error);
            return;
        }
    };
    match sender.send(warp::ws::Message::text(response)).await {
        Ok(()) => (),
        Err(error) => log::error!("{}", error),
    }
}

// setting up a server side logger
fn setup_logger() {
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} - {m}\n")))
        .build("log/output.log")
        .expect("Building Config");

    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(LevelFilter::Info))
        .expect("Building Config");

    log4rs::init_config(config).expect("Initializing Config");
    log::info!("Server Up");
}

// static stuff, this is probably not the right thing to do but it's working for now
lazy_static! {
    static ref TAOS: Taos = TaosBuilder::from_dsn("taos://0.0.0.0:6030")
        .expect("Taos Builder Error")
        .build()
        .expect("Taos Builder Build Error");
}

// initializing and testing a DB insert/select stuff
const DB: &str = "dpp";
async fn setup_db() {
    TAOS.exec_many([
        format!("DROP DATABASE IF EXISTS `{DB}`"),
        format!("CREATE DATABASE `{DB}`"),
        format!("USE `{DB}`"),
        format!("CREATE TABLE {DB} (ts TIMESTAMP, uuid NCHAR(130), ttype NCHAR(64), symbol NCHAR(32), price FLOAT, qty INT)"),
    ])
    .await
    .expect("Selecting database");

    TAOS.exec_many([format!(
        "INSERT INTO {DB} VALUES ('2022-01-01 12:00:01', 'c6b34cd0-c8cd-4a15-bea5-4714fa48df30', 'order', 'AAPL', 152.23, 50)"
    )])
    .await
    .expect("inserting test data");
    let mut result = TAOS
        .query(format!("select * from `{DB}`"))
        .await
        .expect("Query test record");
    let mut rows = taos::AsyncFetchable::rows(&mut result);
    let row = rows
        .try_next()
        .await
        .expect("trying next row")
        .expect("extracting row");
    for (name, value) in row {
        println!("got value of {}: {}", name, value);
    }
}

// async query of a DB (using TDEngine, but postgres or whatever is similar)
async fn get_data() -> Result<Vec<BTreeMap<String, String>>, Error> {
    let mut result = TAOS.query(format!("select * from `{DB}`")).await?;
    let mut rows = result.rows();
    let mut records = vec![BTreeMap::new()];
    while let Some(row) = rows.try_next().await? {
        let mut data = BTreeMap::new();
        for (name, value) in row {
            let val = match value.to_string() {
                Ok(v) => v,
                Err(_) => "".to_string(),
            };
            data.insert(name.to_string(), val);
        }
        records.push(data);
    }
    return Ok(records);
}

// templating html template (mustache) stuff
const DASHBOARD: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Read Data</title> 
    <meta name="author" content="xyz">
    <meta name="application-name" content="ReadData" />
</head>

<body class="text-center">
    <main class="centeredForm">
        {{#each content}}
        <p>
            {{this.ts}}
            {{this.uuid}}
            {{this.ttype}}
            {{this.symbol}}
            {{this.price}}
            {{this.qty}}
        </p>
        {{/each}}
    {{val}}
    </main>
</body>
</html>"#;

struct WithTemplate<T: Serialize> {
    name: &'static str,
    value: T,
}

fn render<T>(template: WithTemplate<T>, hbs: Arc<Handlebars<'_>>) -> impl warp::Reply
where
    T: Serialize,
{
    let render = hbs
        .render(template.name, &template.value)
        .unwrap_or_else(|err| err.to_string());
    warp::reply::html(render)
}

async fn prepare_data(secs: u64) {
    let mut interval = interval(Duration::from_secs(secs));
    interval.tick().await; // skip first tick

    loop {
        interval.tick().await;
        print!("interval tick");
    }
}

// cron scheduler stuff
async fn run_scheduler() {
    println!("run_scheduler");
    let sched = JobScheduler::new().await.expect("Create Scheduler");
    sched
        .add(
            Job::new_async("1/5 * * * * *", |_uuid, l| {
                Box::pin(async move {
                    println!("I run async every 5 seconds");
                    let datares = get_data().await;
                    println!("I got data");
                    let data_vector = match datares {
                        Ok(dv) => dv,
                        Err(_) => vec![],
                    };
                    println!("Iterating");
                    for d in data_vector.iter() {
                        println!("{:?}", d);
                    }
                })
            })
            .expect("new"),
        )
        .await
        .unwrap();
    println!("Starting schedule");
    sched.start().await.expect("Schedule to start");
}

#[tokio::main]
async fn main() {
    setup_db().await;
    setup_logger();
    let dash = warp::path!("dash").and_then({
        move || {
            let mut hb = Handlebars::new();
            hb.register_template_string("template.html", DASHBOARD)
                .unwrap();
            let hb = Arc::new(hb);
            let handlebars = move |with_template| render(with_template, hb.clone());
            async move {
                let data_result = get_data().await;
                let data_vector = match data_result {
                    Ok(dv) => dv,
                    Err(_) => vec![],
                };
                let mut data = BTreeMap::<String, Vec<BTreeMap<String, String>>>::new();
                data.insert("content".to_string(), data_vector);
                let temp = handlebars(WithTemplate {
                    name: "template.html",
                    value: json!(data),
                });
                Ok::<_, Infallible>(temp)
            }
        }
    });
    spawn(prepare_data(60));
    run_scheduler().await;
    let ws = warp::path("ws").and(warp::ws()).map(|ws: warp::ws::Ws| {
        print!("upgrading connection to websocket");
        ws.on_upgrade(handle_ws_client)
    });

    let current_dir = std::env::current_dir().expect("failed to read current directory");
    let routes = warp::get().and(ws.or(dash).or(warp::fs::dir(current_dir)));
    warp::serve(routes)
        .tls()
        .cert_path("cert.pem")
        .key_path("key.rsa")
        .run(([0, 0, 0, 0], 9001))
        .await;
}
