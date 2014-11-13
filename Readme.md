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

# How it works

The data pipeline is comprised of 3 steps (or states, depending on how
you want to look at it).
When the data is submitted to a OONI collector it is synchronized with
the aggregator.
This is a central machine responsible for running all the data
processing tasks, storing the collected data in a database and hosting a
public interface to the sanitised reports. Since all the steps are
independent from one another it is not necessary that they run on the
machine, but it may also be more distributed.

Once the data is on the aggregator machine it is said to be in the RAW
state. The sanitise task is then run on the RAW data to remove sensitive
information and strip out some superfluous information. A RAW copy of
every report is also stored in a private compressed archive for future
reference.
Once the data is sanitised it is said to tbe in SANITISED state. At this
point a import task is run on the data to place it inside of a database.
The SANITISED reports are then place in a directory that is publicly
exposed to the internet to allow people to download also a copy of the
YAML reports.

At this point is is possible to run any export task that performs
queries on the database and produces as output some documents to be used
in the data visualizations (think JSON, CSV, etc.).

