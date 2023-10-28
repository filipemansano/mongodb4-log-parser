use mongodb::options::ClientOptions;
use mongodb::Client;

pub async fn get_client() -> Result<Client, mongodb::error::Error> {
    let mut client_options: ClientOptions =
        ClientOptions::parse("mongodb://localhost:27017").await?;
    client_options.app_name = Some("Benchmark".to_string());
    let client: Client = Client::with_options(client_options)?;

    Ok(client)
}