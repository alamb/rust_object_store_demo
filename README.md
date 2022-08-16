# Introduction
Demonstration of how to use the Rust [object_store](https://docs.rs/object_store/0.4.0/object_store/index.html) crate

# Example Usage:
```shell
# list all files in /tmp
cargo run -- list file:/tmp

# list all files in s3 bucket
AWS_ACCESS_KEY_ID="my_id" AWS_SECRET_ACCESS_KEY="my_secret" AWS_REGION=us-east-1 \
cargo run -- list s3://my-awesome-bucket
```

Example output:
```shell
    Finished dev [unoptimized + debuginfo] target(s) in 2.10s
     Running `/Users/alamb/Software/target-iox/debug/rust_object_store_demo list 'file:/tmp'`
URL: Url { scheme: "file", cannot_be_a_base: false, username: "", password: None, host: None, port: None, path: "/tmp", query: None, fragment: None }
Listing files in 'tmp'...
File name: tmp/calls, size: 4490
File name: tmp/b.csv, size: 4
File name: tmp/calls.read, size: 36214
File name: tmp/s-gcne53r2w3-1tc3l6p.lock, size: 0
File name: tmp/foo.sql, size: 308
File name: tmp/bar.sql, size: 12
File name: tmp/query-cache.bin, size: 5054790
File name: tmp/dep-graph.bin, size: 15732013
File name: tmp/work-products.bin, size: 37
```



# Help wanted

Note that gcp and azure are also supported natively with `object_store`, but I didn't have access to a test account. There are stubs in the program that I would love some help to fill. Please submit a PR and tag me!
