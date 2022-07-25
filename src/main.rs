use std::{num::NonZeroUsize, sync::Arc};

use futures::stream::{FuturesOrdered, StreamExt};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};

//const PREFIX: &str = "/Users/alamb/Software/influxdb_iox/test_fixtures/parquet";
const PREFIX: &str = "/Users/alamb/Software/influxdb_iox";

#[tokio::main]
async fn main() {
    // create an ObjectStore
    let object_store: Arc<dyn ObjectStore> = get_local_store();

    // list all objects in the "parquet" prefix (aka directory)
    //let path: Path = "test_fixtures/parquet".try_into().unwrap();
    let path: Path = "test_fixtures".try_into().unwrap();
    let list_stream = object_store
        .list(Some(&path))
        .await
        .expect("Error listing files");

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
}

fn get_local_store() -> Arc<dyn ObjectStore> {
    let local_fs =
        LocalFileSystem::new_with_prefix(PREFIX).expect("Error creating local file system");
    Arc::new(local_fs)
}

const PATH_TO_SERVICE_ACCOUNT_JSON: &str = "foo";
const BUCKET_NAME: &str = "foo";

fn get_gcs_store() -> Arc<dyn ObjectStore> {
    let gcs = object_store::gcp::new_gcs(PATH_TO_SERVICE_ACCOUNT_JSON, BUCKET_NAME)
        .expect("error creating gcs");
    Arc::new(gcs)
}

const STORAGE_ACCOUNT: &str = "foo";
const ACCESS_KEY: &str = "foo";

fn get_azure_store() -> Arc<dyn ObjectStore> {
    let azure = object_store::azure::new_azure(
        STORAGE_ACCOUNT,
        ACCESS_KEY,
        BUCKET_NAME,
        false, // do not use emulator,
    )
    .expect("error creating azure");
    Arc::new(azure)
}

const ACCESS_KEY_ID: &str = "foo";
const SECRET_KEY: &str = "foo";
const REGION: &str = "foo";

fn get_s3_store() -> Arc<dyn ObjectStore> {
    let s3 = object_store::aws::new_s3(
        Some(ACCESS_KEY_ID),
        Some(SECRET_KEY),
        REGION,
        BUCKET_NAME,
        // TODO this is messy, rust complains about not being able to infer types
        None as Option<&str>, // endpoint
        None as Option<&str>, // token
        NonZeroUsize::new(16).unwrap(),
        false, // allow http
    )
    .expect("error creating s3");
    Arc::new(s3)
}
