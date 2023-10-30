use mongodb::sync::Client;

pub fn get_client(uri: String) -> Client {
    let client = Client::with_uri_str(uri).unwrap();
    client
}