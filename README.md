# `template-rust`

This is a template starter project for showing how to develop
applications using Ditto's Rust SDK.

## About Ditto

[Ditto is an "edge-sync platform"][0], which allows your apps to
work and communicate even while offline. The Ditto SDKs allow
you to read and write changes to a local database even while
offline, but will automatically connect to nearby peers and sync
changes with each other.

- [Ditto Rust API Docs][1]

## Getting Started

To get started with Ditto, [visit the Ditto Portal and create an App][2],
then find your "App ID" and Playground Token. You'll use these to
authenticate your SDK instances so they can securely sync with each other.

Next, copy the `.env.sample` file and rename it to `.env` to use it:

```
cp .env.sample .env
```

Edit the `.env` file and paste your App ID and Playground Token:

```
#!/usr/bin/env bash
# ...

export APP_ID=""         # Your App ID
export SHARED_TOKEN=""   # Your Playground Token
export COLLECTION="cars" # The Collection to watch
```

For now we'll keep the default value for `COLLECTION="cars"`, but you can
change that if you want to try writing documents in different collections.

Now, build and run the sample app with cargo:

```
cargo run
# ...
Inserted document with id=6659efed00288113001bb5a9
```

[0]: https://ditto.live
[1]: https://docs.rs/dittolive-ditto
[2]: https://portal.ditto.live

