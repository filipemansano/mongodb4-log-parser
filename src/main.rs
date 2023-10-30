use std::fs::File;
use std::io::{BufRead, BufReader};
use clap::{Arg, Command};
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use std::sync::mpsc;
use std::time::Instant;
use bson::Document;
use indicatif::ProgressBar;
use ::mongodb::sync::Collection;
use rayon::prelude::*;
mod parser;
mod mongodb;
use parser::Parser;
 
fn mongodb_sender_thread(collection: &Collection<Document>, receiver: mpsc::Receiver<Document>) {
    
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

    if !batch.is_empty() {
        collection
            .insert_many(batch, None)
            .expect("Failed to insert remaining documents");
    }
}

fn main() {
    let matches = Command::new("Log parsser")
        .version("1.0")
        .author("Filipe Mansano")
        .about("MongoDB log file parser TXT version")
        .arg(
            Arg::new("logFile")
             .short('f')
             .long("logFile")
             .value_name("LOG_PATH")
             .help("Specifies the path of the log to be processed")
             .required(true)
        )
        .arg(
            Arg::new("namespace")
             .short('n')
             .long("namespace")
             .value_name("NAMESPACE")
             .help("Specifies the collection namespace <db>.<collection>")
             .required(true)
        )
        .arg(
            Arg::new("uri")
             .short('u')
             .long("uri")
             .value_name("URI")
             .help("Specifies the MongoDB URI to store the parsed logs")
             .required(false)
             .default_value("mongodb://localhost:27017")
        ).get_matches();

    let file_path: String = matches.get_one::<String>("logFile").unwrap().to_string();
    let namespace: String = matches.get_one::<String>("namespace").unwrap().to_string();
    let uri: String = matches.get_one::<String>("uri").unwrap().to_string();
    let colletion: Vec<&str> = namespace.split(".").collect::<Vec<&str>>();

    let client = mongodb::get_client(uri);
    let db = client.database(colletion[0]);
    let collection: Collection<Document> = db.collection(colletion[1]);
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
        mongodb_sender_thread(&collection, receiver);
    });

    let file = File::open(file_path).expect("Falha ao abrir o arquivo");
    let reader = BufReader::new(DecodeReaderBytesBuilder::new()
        .encoding(Some(WINDOWS_1252))
        .build(file));
    let pb = ProgressBar::new_spinner();


    let start = Instant::now();
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