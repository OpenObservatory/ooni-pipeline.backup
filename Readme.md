# OONI Pipeline

This repository contains all the tasks needed to implement the OONI data
analytics pipeline.

![OONI Pipeline](https://raw.githubusercontent.com/TheTorProject/ooni-pipeline/master/_static/OONI-pipeline.png)


# Usage

To run the pipeline you should set the following environment variables:

```
export OONI_RAW_DIR=/data/pipeline/raw/
export OONI_SANITISED_DIR=/data/pipeline/sanitised/
export OONI_PUBLIC_DIR=/data/pipeline/public/
export OONI_ARCHIVE_DIR=/data/pipeline/archive/

export OONI_REMOTE_SERVERS_FILE=/data/pipeline/remote_servers.txt

export OONI_BRIDGE_DB_FILE=/data/pipeline/bridge_db.json

export OONI_DB_IP=127.0.0.1
export OONI_DB_PORT=27017
```

Then you can run the tool with:

```
./bin/oonipipeline sync
```

To check if data should be copied from the remote probes into the RAW directory.

```
./bin/oonipipeline sanitise
```
To move data from the RAW state into the SANITISED state.

```
./bin/oonipipeline import
```

To import the data into the database and publish it to PUBLIC_DIR

```
./bin/oonipipeline export
```

To export the data in the JSON format for the visualisation team.

