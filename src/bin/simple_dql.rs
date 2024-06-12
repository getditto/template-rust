use anyhow::Result;
use clap::{Parser, Subcommand};
use dittolive_ditto::{identity::*, prelude::*, store::dql::QueryResult};
use std::{self, str::FromStr, sync::Arc};

/// A sample app to demo Ditto's Rust SDK, see long '--help' for examples
///
/// This will log into your Ditto app via the AppID and Token
/// you provide. These can be found at <https://portal.ditto.live>.
/// See the README to see how to set up ENVs for the AppID and Token.
///
/// Use the `create-car` or `query-cars` subcommands to explore
/// inserting and querying documents with Ditto
///
/// Example: Insert a new car to the "cars" collection
///
/// > simple_dql create-car --make=ford --year=2016 --color=blue
///
/// Example: Query blue cars from the "cars" collection
///
/// > simple_dql query-cars --color=blue
///
/// If you have not set up ENV variables with your AppID and Token,
/// you can alternatively pass them as arguments like this:
///
/// > simple_dql --app-id="YOUR_APP_ID" --token="YOUR_PLAYGROUND_TOKEN" create-car --make=ford --year=2016 --color=blue
#[derive(Debug, Parser)]
struct Cli {
    #[clap(flatten)]
    args: Args,

    #[clap(subcommand)]
    cmd: Cmd,
}

/// Args needed for any command,
#[derive(Debug, Parser)]
struct Args {
    /// The Ditto App ID to sync with (found at <https://portal.ditto.live>)
    #[clap(long, env = "APP_ID")]
    app_id: String,

    /// The Playground token used to authenticate (found at <https://portal.ditto.live>)
    #[clap(long, env = "PLAYGROUND_TOKEN")]
    playground_token: String,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Create a new "car" document with a make, year, and color
    CreateCar {
        /// The make of the car, e.g. Ford, Chevrolet
        #[clap(long)]
        make: String,

        /// The year of the car
        #[clap(long)]
        year: String,

        /// The color of the car
        #[clap(long)]
        color: String,
    },
    /// Query existing "car" documents based on their color property
    QueryCars {
        /// Query all cars with this color
        #[clap(long)]
        color: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let cli = Cli::parse();

    // Initialize Ditto SDK client
    let args = &cli.args;
    let app_id = AppId::from_str(&args.app_id)?;
    let ditto = Ditto::builder()
        .with_root(Arc::new(PersistentRoot::from_current_exe()?))
        .with_minimum_log_level(LogLevel::Debug)
        .with_identity(move |ditto_root| {
            let shared_token = args.playground_token.clone();
            let enable_cloud_sync = true;
            let custom_auth_url = None;
            OnlinePlayground::new(
                ditto_root,
                app_id,
                shared_token,
                enable_cloud_sync,
                custom_auth_url,
            )
        })?
        .build()?;

    // Begin sync, then open the Ditto Store so we can insert or query documents
    ditto.start_sync()?;
    let store = ditto.store();

    match cli.cmd {
        Cmd::CreateCar { make, year, color } => {
            let result_set = create_car(store, &make, &year, &color).await?;
            let mutations = result_set.mutated_document_ids();
            let s = if mutations.len() == 1 { "" } else { "s" };
            println!("Mutated {} document{s}", mutations.len());
        }
        Cmd::QueryCars { color } => {
            let result_set = query_cars(store, &color).await?;
            let count = result_set.item_count();
            println!("Found {count} cars with color={}", color);

            // Print the contents of each queried document
            for item in result_set.iter() {
                let json = item.deserialize_value::<serde_json::Value>()?;
                println!("Car with color={}: {json}", color);
            }
        }
    }

    Ok(())
}

async fn create_car(store: &Store, make: &str, year: &str, color: &str) -> Result<QueryResult> {
    let result_set = store
        .execute(
            "INSERT INTO cars DOCUMENTS (:newCar)",
            Some(
                serde_json::json!({
                    "newCar": {
                        "make": make,
                        "year": year,
                        "color": color
                    }
                })
                .into(),
            ),
        )
        .await?;

    Ok(result_set)
}

async fn query_cars(store: &Store, color: &str) -> Result<QueryResult> {
    let query_args = serde_json::json!({
        "color": color,
    });

    // Execute a DQL query and get a result set
    let result_set = store
        .execute(
            "SELECT * FROM cars where color = :color",
            Some(query_args.into()),
        )
        .await?;

    Ok(result_set)
}
