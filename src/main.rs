use std::sync::Arc;

use futures::stream::{FuturesOrdered, StreamExt};
use object_store::{
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem, path::Path, ObjectStore,
};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        let n = &args[0];
        eprintln!(
            "USAGE: {} <list|zeros> <url>\n\
             Examples: \n\
             {} zeros file:/temp\n\
             {} list s3://my-awesome-bucket",
            n, n, n
        );
        return Err("Invalid command line".to_string());
    }

    let url =
        Url::parse(&args[2]).map_err(|e| format!("Error parsing URL: '{}': {}", args[2], e))?;

    // create an ObjectStore
    let object_store = get_object_store(&url)?;

    let path: Path = url
        .path()
        .try_into()
        .map_err(|e| format!("Unsupported path: '{}': {}", url.path(), e))?;

    match args[1].as_ref() {
        "list" => list_demo(object_store, path).await,
        "zeros" => zeros_demo(object_store, path).await,
        _ => Err(format!(
            "Unknown command '{}'. Expected 'list' or 'zeros'",
            args[1]
        )),
    }
}

/// Demonstrate how to count zeros in the data on a remote file system
async fn zeros_demo(object_store: Arc<dyn ObjectStore>, path: Path) -> Result<(), String> {
    let list_stream = object_store
        .list(Some(&path))
        .await
        .map_err(|e| format!("Error listing files in: '{}': {}", path, e))?;

    // List all files in the store
    list_stream
        .map(|meta| async {
            let meta = meta.expect("Error listing");

            // fetch the bytes from object store
            let stream = object_store
                .get(&meta.location)
                .await
                .unwrap()
                .into_stream();

            // Count the zeros
            let num_zeros = stream
                .map(|bytes| {
                    let bytes = bytes.unwrap();
                    bytes.iter().filter(|b| **b == 0).count()
                })
                .collect::<Vec<usize>>()
                .await
                .into_iter()
                .sum::<usize>();

            (meta.location.to_string(), num_zeros)
        })
        .collect::<FuturesOrdered<_>>()
        .await
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .for_each(|i| println!("{} has {} zeros", i.0, i.1));

    Ok(())
}

/// Demonstrate how to list files on a remote filesystem
async fn list_demo(object_store: Arc<dyn ObjectStore>, path: Path) -> Result<(), String> {
    println!("Listing files in '{}'...", path);

    let list_stream = object_store
        .list(Some(&path))
        .await
        .map_err(|e| format!("Error listing files in '{}': {}", path, e))?;

    // List all files in the store
    list_stream
        .for_each(move |meta| async {
            let meta = meta.expect("Error listing");
            println!("File name: {}, size: {}", meta.location, meta.size);
        })
        .await;
    Ok(())
}

/// return an object store based on url
///
/// file:/path --> local file
/// s3://bucket_name/path --> s3
///
fn get_object_store(url: &Url) -> Result<Arc<dyn ObjectStore>, String> {
    println!("URL: {:?}", url);
    match url.scheme() {
        "file" => {
            if url.host().is_some() {
                Err(format!(
                    "Unsupported file url. Expected no host: {}",
                    url.as_str()
                ))
            } else {
                Ok(Arc::new(LocalFileSystem::new()))
            }
        }
        "s3" => {
            let host = url.host().ok_or_else(|| {
                format!("unsupported s3 url. Expected bucket name: {}", url.as_str())
            })?;

            get_s3_store(&host.to_string())
        }
        "gcs" => {
            Err("GCS support not yet hooked up due to lack of testing. Help wanted!".to_string())
        }
        "azure" => {
            Err("azure support not yet hooked up due to lack of testing. Help wanted!".to_string())
        }
        _ => Err("Unsupported url. Try file:/foo, s3://bucket, gcs://bucket".to_string()),
    }
}

const PATH_TO_SERVICE_ACCOUNT_JSON: &str = "foo";
const BUCKET_NAME: &str = "foo";

fn get_gcs_store() -> Arc<dyn ObjectStore> {
    let gcs = GoogleCloudStorageBuilder::new()
        .with_service_account_path(PATH_TO_SERVICE_ACCOUNT_JSON)
        .with_bucket_name(BUCKET_NAME)
        .build()
        .expect("error creating gcs");
    Arc::new(gcs)
}

const STORAGE_ACCOUNT: &str = "foo";
const ACCESS_KEY: &str = "foo";

fn get_azure_store() -> Arc<dyn ObjectStore> {
    let azure = MicrosoftAzureBuilder::new()
        .with_account(STORAGE_ACCOUNT)
        .with_access_key(ACCESS_KEY)
        .with_container_name(BUCKET_NAME)
        .build()
        .expect("error creating azure");

    Arc::new(azure)
}

fn get_s3_store(bucket_name: &str) -> Result<Arc<dyn ObjectStore>, String> {
    let AWS_ACCESS_KEY_ID = std::env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| "Error creating s3: AWS_ACCESS_KEY environment not set".to_string())?;

    let AWS_SECRET_ACCESS_KEY = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| "Error creating s3: AWS_SECRET_ACCESS_KEY environment not set".to_string())?;

    // default to eu-central-1 region
    let AWS_REGION = std::env::var("AWS_REGION").unwrap_or("eu-central-1".to_string());

    let s3 = AmazonS3Builder::new()
        .with_access_key_id(AWS_ACCESS_KEY_ID)
        .with_secret_access_key(AWS_SECRET_ACCESS_KEY)
        .with_region(AWS_REGION)
        .with_bucket_name(bucket_name)
        .build()
        .expect("error creating s3");

    Ok(Arc::new(s3))
}
