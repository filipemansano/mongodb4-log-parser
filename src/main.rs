use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use std::time::Instant;
use bson::Document;
use indicatif::ProgressBar;
use ::mongodb::sync::Collection;
use rayon::prelude::*;
mod parser;
mod mongodb;
use parser::Parser;
 
fn mongodb_sender_thread(receiver: mpsc::Receiver<Document>) {
    let client = mongodb::get_client();
    let db = client.database("ahgora");
    let collection: Collection<Document> = db.collection("logs4");
    let batch_size = 1000;
    let mut batch = Vec::with_capacity(batch_size);

    for doc in receiver {

        if batch.len() == batch_size {
            collection
                .insert_many(batch.clone(), None)
                .expect("Failed to insert documents");
            batch.clear();
        }

        batch.push(doc);
    }

    // Inserir o lote restante, se houver
    if !batch.is_empty() {
        print!("Inserting remaining documents...");
        collection
            .insert_many(batch, None)
            .expect("Failed to insert remaining documents");
    }
}

fn main() {
    let start = Instant::now();
    let file_path = "/Users/filipemansano/MongoDB/tests/log-text/src/atlas.log";

    let num_threads = 10;

    let (sender, receiver): (
        mpsc::Sender<Document>,
        mpsc::Receiver<Document>,
    ) = mpsc::channel();

    let parser = Parser::new();

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap();

    let mongodb_thread = std::thread::spawn(move || {
        mongodb_sender_thread(receiver);
    });

    let file = File::open(file_path).expect("Falha ao abrir o arquivo");
    let reader = BufReader::new(file);
    let pb = ProgressBar::new_spinner();

    reader
        .lines()
        .enumerate()
        .par_bridge()
        .for_each_with((sender, parser), |(sender, parser), (line_number, line)|{
            let line = line.expect("Error on read line");
            
            let doc = parser.parse_line(&line);
            if doc.is_ok() {
                sender.send(doc.unwrap()).expect("Failed to queue document for sending");
            }

            pb.set_message(&format!("Total lines read: {}", line_number));
        });

    mongodb_thread.join().expect("Failed to join MongoDB thread");
    
    pb.finish_with_message("Done");
    print!("Total time: {} seconds", start.elapsed().as_secs());
}