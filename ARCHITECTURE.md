# Disney Ad Ops Lab: System Architecture & Sub-System Design

This document provides a deep dive into the architectural design of the automation tools built for the Disney Ad Ops Lab. It breaks down the overall system flow and then explores each Python module's sub-system design using visual diagrams.

---

## 🏗️ 1. Overall System Architecture

The Disney Ad Ops Lab is designed as a multi-tier architecture connecting a human-facing operational UI (BOAT/Airtable) with a heavy-duty data lakehouse (Databricks) via a centralized automation brain (EVE Orchestrator).

```mermaid
flowchart TD
    %% Styling
    classDef frontend fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#fff
    classDef engine fill:#065f46,stroke:#10b981,stroke-width:2px,color:#fff
    classDef backend fill:#701a75,stroke:#d946ef,stroke-width:2px,color:#fff
    classDef database fill:#9a3412,stroke:#f97316,stroke-width:2px,color:#fff
    classDef decision fill:#854d0e,stroke:#eab308,stroke-width:2px,color:#fff,shape:diamond
    classDef external fill:#1f2937,stroke:#6b7280,stroke-width:2px,color:#fff,stroke-dasharray: 5 5

    subgraph Frontend [BOAT: Human Operations]
        A[Airtable UI]:::frontend --> |Data Entry| B[(Airtable Database)]:::database
        B --> |Real-time Sync| C[API Endpoints]:::frontend
    end

    subgraph Automations [EVE: Python Automation Engine]
        C <--> |REST API| D[Airtable Client]:::engine
        D --> E[Master Orchestrator]:::engine
        E --> F[Trafficking Engine]:::engine
        F --> G[QA Engine]:::engine
        G --> H{Pass/Fail?}:::decision
        H -->|Pass| I[Live API Translation]:::engine
        H -->|Fail| J[Alerting Pipeline]:::engine
    end

    subgraph Backend [Data Lakehouse]
        K[Data Generator]:::backend --> |CSVs| L[Databricks Volumes]:::database
        L --> M[Spark / SQL Ingestion]:::backend
        M --> N[(Delta Tables)]:::database
        N --> O[Campaign Dashboards]:::backend
    end
    
    J --> |Webhooks| Slack[Teams / Slack]:::external
    I -.- |Future Phase 2| API[External Ad APIs: Meta, TikTok]:::external
```

---

## 🧩 2. Sub-System Designs

### A. Data Generation Sub-System (`data_generator.py`)
This sub-system simulates historical Disney data. It ensures we have perfectly modeled reference taxonomies and massive volumes of delivery data (impressions, clicks) to test the analytical limits of Databricks.

```mermaid
flowchart LR
    %% Styling
    classDef ref fill:#1e40af,stroke:#60a5fa,color:#fff,rx:5px,ry:5px
    classDef oper fill:#0f766e,stroke:#2dd4bf,color:#fff,rx:5px,ry:5px
    classDef data fill:#86198f,stroke:#e879f9,color:#fff,rx:5px,ry:5px
    classDef csv fill:#3f3f46,stroke:#a1a1aa,color:#fff,shape:cylinder

    A[Reference Dictionaries]:::ref --> B[Generate Core Entities]:::ref
    B --> C(Markets, Brands, Channels):::ref
    B --> D(Users, Ticket Types):::ref
    C --> E[Generate Operational Data]:::oper
    D --> E
    E --> F(Titles, Campaigns, Tickets):::oper
    F --> G[Generate Big Data]:::data
    G --> H[(delivery.csv)]:::csv
    G --> I[(qa_checks.csv)]:::csv
```

### B. Airtable Client Intermediary (`src/airtable/client.py`)
This wrapper acts as the strictly typed interface between the raw HTTP responses of `pyairtable` and our internal Python domain logic.

```mermaid
classDiagram
    %% Styling
    class AirtableClient {
        -api: pyairtable.Api
        -base_id: String
        +get_pending_tickets() List
        +get_campaign(campaign_id) Dict
        +update_ticket_stage(record_id, new_stage)
        +create_qa_check(ticket_id, result, details)
        -_get_table(table_name)
    }
    
    class pyairtable {
        <<Library>>
        +Api(pat)
        +table(base, name)
    }

    class AirtableAPI {
        <<Cloud Service>>
        +GET /v0/{base}/{table}
        +PATCH /v0/{base}/{table}
        +POST /v0/{base}/{table}
    }

    AirtableClient --> pyairtable : Wraps Authentication (.env)
    pyairtable --> AirtableAPI : REST JSON Interop
```

### C. EVE Trafficking Engine (`src/trafficking/engine.py`)
This is the core business logic. It translates human instructions from Airtable into machine-readable actions and standardizes Disney naming conventions.

```mermaid
flowchart TD
    %% Styling
    classDef input fill:#374151,stroke:#9ca3af,stroke-width:2px,color:#fff
    classDef logic fill:#0369a1,stroke:#38bdf8,stroke-width:2px,color:#fff
    classDef output fill:#14532d,stroke:#4ade80,stroke-width:2px,color:#fff
    classDef decision fill:#854d0e,stroke:#facc15,stroke-width:2px,color:#fff

    A[Raw Ticket Data from Airtable]:::input --> B{Determine Request Type}:::decision
    B -->|New Campaign| C[Extract Campaign Fields]:::logic
    B -->|Retrafficking| D[Extract Placement Fields]:::logic
    B -->|Other| E[No Automated Action Required]:::logic
    
    C --> F[Determine Best Platform API]:::logic
    F --> G[Construct standard Disney Taxonomy]:::logic
    G --> H[Generate JSON Trafficking Payload]:::logic
    H --> I[Return List of Payloads to Orchestrator]:::output
    D --> F
```

### D. Automated QA Engine (`src/trafficking/qa_engine.py`)
The safety net. Before any generated payload hits the internet, it must pass a suite of rigid compliance checks to prevent brand safety issues or massive budget losses.

```mermaid
flowchart TD
    %% Styling
    classDef payload fill:#4b5563,stroke:#d1d5db,stroke-width:2px,color:#fff
    classDef test fill:#0f766e,stroke:#5eead4,stroke-width:2px,color:#fff,shape:diamond
    classDef fail fill:#7f1d1d,stroke:#fca5a5,stroke-width:2px,color:#fff
    classDef pass fill:#14532d,stroke:#86efac,stroke-width:2px,color:#fff

    A[Generated Payload]:::payload --> B[Execute Test Suite]:::payload
    B --> C{Check 1: Geo Targeting}:::test
    C -->|Fail| Z[Append Failure Log]:::fail
    C -->|Pass| D{Check 2: Taxonomy Validity}:::test
    D -->|Fail| Z
    D -->|Pass| E{Check 3: Spec Compliance}:::test
    E -->|Fail| Z
    E -->|Pass| Y[Append Pass Log]:::pass
    
    Z --> F[Return Aggregated Results]:::payload
    Y --> F
```

### E. Alerting Pipeline (`src/alerting/pipeline.py`)
Handles asynchronous communication with human teams when the orchestrator encounters blocked workflows or SLA breaches.

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant P as AlertPipeline
    participant W as Slack/Teams Webhook
    
    O->>P: send_qa_failure_alert(Ticket, Fails)
    Note over P: Format Markdown Payload<br/>Append Mentions
    P->>W: POST /webhook/url (Failure Trigger)
    W-->>P: 200 OK
    
    O->>P: send_sla_breach_alert(Tickets)
    Note over P: Aggregate breached users<br/>Rank by Severity
    P->>W: POST /webhook/url (SLA Breach)
    W-->>P: 200 OK
```

### F. The Master Orchestrator (`src/orchestrator.py`)
The infinite loop that binds all sub-systems. It fetches work, processes it, QA's it, and writes the results back.

```mermaid
sequenceDiagram
    autonumber
    participant UI as AirtableUI
    participant O as Orchestrator
    participant TE as TraffickingEngine
    participant QA as QAEngine
    participant AP as AlertPipeline

    O->>UI: get_pending_tickets()
    UI-->>O: List of pending [Trafficking] Tickets
    
    loop For Each Pending Ticket
        O->>UI: get_campaign(campaign_id)
        UI-->>O: Mapped Campaign Fields
        
        O->>TE: process_ticket(ticket, campaign)
        Note over TE: Transforms logic<br/>Builds taxonomy
        TE-->>O: Generated Playloads Array
        
        O->>QA: run_all_checks(payloads, campaign)
        Note over QA: Performs strict check rules
        QA-->>O: QA Results Array [Pass/Fail]
        
        O->>UI: create_qa_check() logs for traceability
        
        alt All Checks Passed
            O->>UI: update_ticket_stage("Ready to Launch")
        else Any Check Failed
            O->>UI: update_ticket_stage("QA")
            O->>AP: TRIGGER Notification to assigned trafficker
        end
    end
```
