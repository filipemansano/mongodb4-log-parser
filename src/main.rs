use std::env;
use std::error::Error;
use ::mongodb::Client;
use ::mongodb::Database;
use ::mongodb::Collection;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

mod parser;
mod mongodb;
use parser::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client: Client = mongodb::get_client().await?;
    let db: Database = client.database("ahgora");
    let collection: Collection<bson::Document> = db.collection("logs2");

    let mut batch: Vec<bson::Document> = Vec::with_capacity(5000);
    let mut c: i32 = 0;

    let num_threads = 10;

    let mut handles = vec![];
    let mut parser = Parser::new();

    for _ in 0..num_threads {
        let (tx, mut rx): (
            mpsc::Sender<Vec<bson::Document>>,
            mpsc::Receiver<Vec<bson::Document>>,
        ) = mpsc::channel(1000);

        let collection = collection.clone();
        let insert_task = tokio::spawn(async move {
            while let Some(docs) = rx.recv().await {
                if !docs.is_empty() {
                    collection.insert_many(docs, None).await.unwrap();
                }
            }
        });
        handles.push((tx, insert_task));
    }

    let mut line_stream = get_stream_line().await?;

    while let Some(line) = line_stream.next_line().await? {
        c += 1;
        let doc = parser.parse_line(line);

        if doc.is_err() {
            println!("Error: {}, line {}", doc.err().unwrap(), c);
            continue;
        }

        batch.push(doc.unwrap());

        if batch.len() == 5000 {
            let insert_task_index = (c % num_threads) as usize;
            let (tx, _) = &mut handles[insert_task_index];
            let batch_copy = batch.clone();
            let send_result = tx.send(batch_copy).await;
            if send_result.is_err() {
                println!("Error sending batch to insert task");
            }
            batch.clear();
        }
    }

    for (tx, handle) in handles {
        let send_result = tx.send(batch.clone()).await;
        if send_result.is_err() {
            println!("Error sending remaining batch to insert task");
        }

        handle.await.unwrap();
    }

    Ok(())
}

async fn get_stream_line() -> Result<tokio::io::Lines<BufReader<File>>, Box<dyn Error>> {
    let current_dir: std::path::PathBuf = env::current_dir()?;
    let file_path: std::path::PathBuf = current_dir.join("src/atlas.log");

    let file = File::open(file_path).await?;
    let reader = BufReader::new(file);

    let line_stream: tokio::io::Lines<BufReader<File>> = reader.lines();
    Ok(line_stream)
}