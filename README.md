# Merritt Storage

This microservice is part of the [Merritt Preservation System](https://github.com/CDLUC3/mrt-doc).

## Purpose

This microservice provides an API for other Merritt microservices to interact with Merritt's cloud storage providers.

This microservice supports the ingest of new objects into the Merritt Preservation System 
and the retrieval of content from the Merritt Preservation System.

## Original System Specifications
- [Merritt Storage Service](https://github.com/CDLUC3/mrt-doc/blob/main/doc/Merritt-storage-service-latest.pdf)

## Component Diagram

### Merritt Store - Ingest Content

```mermaid
%%{init: {'theme': 'neutral', 'securityLevel': 'loose', 'themeVariables': {'fontFamily': 'arial'}}}%%
graph TD
  ING(Ingest)
  click ING href "https://github.com/CDLUC3/mrt-ingest" "source code"
  ST(Storage)
  click ST href "https://github.com/CDLUC3/mrt-store" "source code"
  INV(Inventory)
  click INV href "https://github.com/CDLUC3/mrt-inventory" "source code"
  ZOOINV>Zookeeper Inventory]
  click ZOOINV href "https://github.com/CDLUC3/mrt-zoo" "source code"

  subgraph flowchart

    subgraph cloud_storage
      CLOUD(("Cloud Storage - Primary Node"))
      click CLOUD href "https://github.com/CDLUC3/mrt-cloud" "source code"
    end

    ING --> |deposit| ST
    INV --> |retrieve manifest| ST
    ST -.-> |manifest file| INV
    ST --> |deposit to primary node| CLOUD
    CLOUD --> |retrieve manifest| ST
    ZOOINV --> INV 
  end
  style CLOUD fill:#77913C
  style ST stroke:red,stroke-width:4px
  style ZOOINV fill:cyan
```

### Merritt Access - File Retrieval

```mermaid
%%{init: {'theme': 'neutral', 'securityLevel': 'loose', 'themeVariables': {'fontFamily': 'arial'}}}%%
graph TD
  UI("Merritt UI")
  click UI href "https://github.com/CDLUC3/mrt-dashboard" "source code"
  ST(Storage - Access)
  click ST href "https://github.com/CDLUC3/mrt-store" "source code"
  BROWSER[[Browser]]

  subgraph flowchart

    subgraph cloud_storage
      CLOUD(("Cloud Storage - Primary Node"))
      click CLOUD href "https://github.com/CDLUC3/mrt-cloud" "source code"
    end

    BROWSER --> |retrieval req| UI
    UI -.-> |presigned redirect| BROWSER
    UI --> |retrieval req| ST
    ST --> |retrieval req| CLOUD
    CLOUD -.-> |presigned URL| ST
    ST -.-> |presigned URL| UI
    CLOUD -.-> |presigned retrieval| BROWSER
  end
  style CLOUD fill:#77913C
  style ST stroke:red,stroke-width:4px
```

### Merritt Access - Object Retrieval

```mermaid
%%{init: {'theme': 'neutral', 'securityLevel': 'loose', 'themeVariables': {'fontFamily': 'arial'}}}%%
graph TD
  UI("Merritt UI")
  click UI href "https://github.com/CDLUC3/mrt-dashboard" "source code"
  BROWSER[[Browser]]

  subgraph flowchart
 
    subgraph Storage_Access
      RO(Request Object)
      AO(Assemble Object)
      ZOOACC>Zookeeper Access Assembly]
      click ZOOACC href "https://github.com/CDLUC3/mrt-zoo" "source code"
      ASSDAEMON[[Object Assembly Daemon]]
      CT(Check Token)
      RP(Return Presigned)
    end
 
    subgraph cloud_storage
      CLOUD(("Cloud Storage - Primary Node"))
      click CLOUD href "https://github.com/CDLUC3/mrt-cloud" "source code"
      ASSM[[Object Assembly]]
      CLEAN[S3 Retention Policy]
      CLEAN --> |automatic deletion| ASSM
    end

    BROWSER --> |retrieval req| UI
    UI -.-> |presigned redirect| BROWSER
    UI --> |retrieval req| RO
    UI --> |check assembly| CT
    RO --> AO
    RO -.-> |assembly token| UI
    CLOUD --> |retrieve file| AO
    AO --> |queue assembly| ZOOACC
    ZOOACC --> ASSDAEMON
    ASSDAEMON --> |object assembly| ASSM
    CT -.-> |status check| AO
    ASSM --> |presigned URL| RP
    RP -.-> |presigned URL| UI
    ASSM -.-> |presigned retrieval| BROWSER
    CT --> RP
  end
  style CLOUD fill:#77913C
  style Storage_Access stroke:red,stroke-width:4px
  style CLEAN fill:cyan
  style ZOOACC fill:cyan
```

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
- https://github.com/CDLUC3/mrt-doc-private/blob/main/uc3-mrt-store.md
 
