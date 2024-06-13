use anyhow::Result;
use clap::{Parser, Subcommand};
use dittolive_ditto::{identity::*, prelude::*, store::dql::QueryResult};
use serde::{Deserialize, Serialize};
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
/// > `simple_dql insert-car --make=ford --color=blue`
///
/// Example: Select blue cars from the "cars" collection
///
/// > `simple_dql select-cars --color=blue`
///
/// If you have not set up ENV variables with your AppID and Token,
/// you can alternatively pass them as arguments like this:
///
/// > `simple_dql --app-id="YOUR_APP_ID" --token="YOUR_PLAYGROUND_TOKEN" create-car --make=ford --year=2016 --color=blue`
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
    InsertCar {
        /// The color of the car
        #[clap(long)]
        color: String,

        /// The make of the car, e.g. Ford, Chevrolet
        #[clap(long)]
        make: String,
    },
    /// Query existing "car" documents based on their color property
    SelectCars {
        /// Query all cars with this color
        #[clap(long)]
        color: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok(); // Load variables from .env
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

    match cli.cmd {
        Cmd::InsertCar { make, color } => {
            let car = Car { color, make };
            let result_set = dql_insert_car(&ditto, &car).await?;
            let mutations = result_set.mutated_document_ids();
            let s = if mutations.len() == 1 { "" } else { "s" };
            println!("Inserted {} car{s}", mutations.len());
        }
        Cmd::SelectCars { color } => {
            let cars = dql_select_cars(&ditto, &color).await?;
            println!("Selected {} cars with color={color}", cars.len());

            // Print the contents of each queried document
            for car in &cars {
                println!("Car with color={color}: {car:?}");
            }
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Car {
    pub color: String,
    pub make: String,
}

async fn dql_insert_car(ditto: &Ditto, car: &Car) -> Result<QueryResult> {
    let store = ditto.store();
    let query_result = store
        .execute(
            "INSERT INTO cars DOCUMENTS (:newCar)",
            Some(
                serde_json::json!({
                    "newCar": car
                })
                .into(),
            ),
        )
        .await?;

    Ok(query_result)
}

/// Execute a DQL query and get a result set
async fn dql_select_cars(ditto: &Ditto, color: &str) -> Result<Vec<Car>> {
    let store = ditto.store();
    let query_result = store
        .execute(
            "SELECT * FROM cars where color = :myColor",
            Some(
                serde_json::json!({
                    "myColor": color
                })
                .into(),
            ),
        )
        .await?;

    let cars = query_result
        .iter()
        .map(|query_item| query_item.deserialize_value::<Car>())
        .collect::<Result<Vec<Car>, _>>()?;

    Ok(cars)
}
