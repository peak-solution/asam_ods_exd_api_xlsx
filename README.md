# ASAM ODS EXD-API XLSX plugin

This repository contains a [ASAM ODS EXD-API](https://www.asam.net/standards/detail/ods/) plugin that uses pandas to read the Excel `*.xlsx` files.

> This is a prove of concept.

## Feature

* Load sheet from XLSX file into groups and columns into channels.
* Supports datetime, string, integer and floating point numbers.
** Import integer column data into float64 to avoid int64 returned by pandas.
* Try to determine unit and description from rows leading rows.

## Content

### Implementation
* [exd_api_server.py](exd_api_server.py)<br>
  Runs the GRPC service to be accessed using http-2.
* [external_data_reader.py](external_data_reader.py)<br>
  Implements the EXD-API interface to access XLSX files using pandas.

### Tests
* [test_exd_api.py](test/test_exd_api.py)<br>
  Some basic tests on example files in `data` folder.
* [example_access_exd_api.ipynb](example_access_exd_api.ipynb)<br>
  jupyter notebook the shows communication done by ASAM ODS server or Importer using the EXD-API plugin.

## GRPC stub

The repository contains the stubs generated from ASAM ODS protobuf files.
The files that match `*_pb2*` are generated using the following command. To renew them you must put the
[proto files from the ODS standard](https://github.com/asam-ev/ASAM-ODS-Interfaces) into `proto_src` and rerun the command.

```
python -m grpc_tools.protoc --proto_path=proto_src --pyi_out=. --python_out=. --grpc_python_out=. ods.proto ods_external_data.proto
```

The Docker file contains generation by pulling the interface files from its origin repository.

## Usage in ODS Server

```mermaid
sequenceDiagram

actor CLIENT as Client
participant PDTT as 🛠️Importer
participant PODS as 🗃️ASAM ODS server
participant PLUGIN as 📊EXD-API plugin
participant FILE as 🗂️File Storage

autonumber

opt Import phase
  FILE ->>+ PDTT: New file shows up
  PDTT ->>+ PLUGIN : Get Structure
  PLUGIN -> FILE: Extract content information
  PLUGIN ->> PLUGIN: Create Structure
  PLUGIN ->>- PDTT: Return Structure
  PDTT ->> PODS: Import ODS structure
  Note right of PDTT: Create hierarchy<br/>AoTest,AoMeasurement,...
  PDTT ->>- PODS: Add External Data info
  Note right of PDTT: Attach AoFile ... for external data<br/>AoFile,AoSubmatrix,AoLocalColumn,...
end

Note over CLIENT, FILE: Now we can work with the imported files

loop Runtime phase
  CLIENT ->> PODS: Establish ODS session
  CLIENT ->> PODS: Work with meta data imported from structure
  CLIENT ->> PODS: Access external channel in preview
  PODS ->> PLUGIN: GetValues
  PLUGIN ->> FILE: Get Channel values
  PLUGIN ->> PODS: Return values of channels
  PODS ->> CLIENT: Return values needed for plot
end
```

## Docker

### Docker Image Details

The Docker image for this project is available at:

`ghcr.io/peak-solution/asam-ods-exd-api-xlsx:latest`

This image is automatically built and pushed via a GitHub Actions workflow. To pull and run the image:

```
docker pull ghcr.io/peak-solution/asam-ods-exd-api-xlsx:latest
docker run -v /path/to/local/data:/data -p 50051:50051 ghcr.io/peak-solution/asam-ods-exd-api-xlsx:latest
```

### Using the Docker Container

To build the Docker image locally:
```
docker build -t asam-ods-exd-api-xlsx .
```

To start the Docker container:
```
docker run -v /path/to/local/data:/data -p 50051:50051 asam-ods-exd-api-xlsx
```