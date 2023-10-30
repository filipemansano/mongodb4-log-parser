use mongodb::sync::Client;

pub fn get_client() -> Client {
    let client = Client::with_uri_str("mongodb://localhost:27017").unwrap();
    client
}