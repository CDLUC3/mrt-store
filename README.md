# Merritt Storage

This microservice is part of the [Merritt Preservation System](https://github.com/CDLUC3/mrt-doc).

## Purpose

This microservice provides an API for other Merritt microservices to interact with Merritt's cloud storage providers.

This microservice supports the ingest of new objects into the Merritt Preservation System 
and the retrieval of content from the Merritt Preservation System.

## Component Diagram

### Merritt Store - Ingest Content
![Flowchart](https://github.com/CDLUC3/mrt-doc/raw/master/diagrams/store-ing.mmd.svg)

### Merritt Access - File Retrieval
![Flowchart](https://github.com/CDLUC3/mrt-doc/raw/master/diagrams/store-file.mmd.svg)

### Merritt Access - Object Retrieval
![Flowchart](https://github.com/CDLUC3/mrt-doc/raw/master/diagrams/store-obj.mmd.svg)

## Dependencies

This code depends on the following Merritt Libraries.
- [Merritt Cloud API](https://github.com/CDLUC3/mrt-cloud)
- [Merritt Core Library](https://github.com/CDLUC3/mrt-core2)

## For external audiences
This code is not intended to be run apart from the Merritt Preservation System.

See [Merritt Docker](https://github.com/CDLUC3/merritt-docker) for a description of how to build a test instnce of Merritt.

## Build instructions
This code is deployed as a war file. The war file is built on a Jenkins server.

## Test instructions

## Internal Links
