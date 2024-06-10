use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use dittolive_ditto::{identity::*, prelude::*};
use std::{
    self,
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

/// A sample app to demo Ditto's Rust SDK, see long '--help' for examples
///
/// This will log into your Ditto app via the AppID and Token
/// you provide. These can be found at <https://portal.ditto.live>.
/// See the README to see how to set up ENVs for the AppID and Token.
///
/// Use the `upload-photo` and `download-photo` commands to explore using
/// Ditto attachments
///
/// Example: upload a photo
///
/// > simple_attachment upload-photo --path=$HOME/Downloads/photo.png
///
/// Example: download a photo by name
///
/// > simple_attachment download-photo --name=photo.png
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
    /// The Ditto App ID to sync with (found at portal.ditto.live)
    #[clap(long, env = "APP_ID")]
    app_id: String,

    /// The Playground token used to authenticate (found at portal.ditto.live)
    #[clap(long, env = "PLAYGROUND_TOKEN")]
    playground_token: String,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    UploadPhoto {
        /// Path to a file to upload as an attachment
        #[clap(long)]
        path: PathBuf,
    },
    DownloadPhoto {
        /// Name of the attachment file to download
        #[clap(long)]
        name: String,
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
        Cmd::UploadPhoto { path } => {
            upload_photo(store, &path).await?;
        }
        Cmd::DownloadPhoto { name } => {
            download_photo(store, &name).await?;
        }
    }

    Ok(())
}

async fn upload_photo(store: &Store, path: &Path) -> Result<()> {
    let photo_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("photo");

    let photo_attachment = store.new_attachment(path, HashMap::default()).await?;
    let _result = store
        .execute(
            "INSERT INTO COLLECTION photos (photo_attachment ATTACHMENT) DOCUMENTS (:photo_doc)",
            Some(
                serde_json::json!({
                    "photo_doc": {
                        "photo_name": photo_name,
                        "photo_attachment": photo_attachment
                    }
                })
                .into(),
            ),
        )
        .await?;

    println!("Uploaded photo with name '{photo_name}'");
    Ok(())
}

async fn download_photo(store: &Store, name: &str) -> Result<()> {
    let result = store
        .execute(
            "SELECT * FROM COLLECTION photos (photo_attachment ATTACHMENT)",
            Some(
                serde_json::json!({
                    "photo_name": name
                })
                .into(),
            ),
        )
        .await?;

    let result_item = result
        .get_item(0)
        .context("result set contained no items")?
        .value();

    let photo_attachment = result_item
        .get("photo_attachment")
        .context("failed to find photo_attachment")?;

    let photo_attachment_token = photo_attachment
        .as_object()
        .context("failed to get attachment token")?;

    let photo_id = photo_attachment
        .get("id")
        .context("failed to get ID of attachment")?
        .clone(); // Cloned to move into closure below

    let _fetcher = store.fetch_attachment(photo_attachment_token, move |event| {
        use DittoAttachmentFetchEvent::*;
        match event {
            Progress {
                downloaded_bytes,
                total_bytes,
            } => {
                println!("Fetcher progress for attachment {photo_id:?}: {downloaded_bytes}b/{total_bytes}b");
            }
            Completed { attachment } => {
                println!("Successfully downloaded attachment {photo_id:?} to path {}", attachment.path().display());
            }
            Deleted => unreachable!("attachment should not get deleted while fetching"),
        }
    })?;

    Ok(())
}
