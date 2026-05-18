# KB Pipeline Architecture — US2.1 → US2.4

> **Backlinks:** [CLAUDE.md](../../CLAUDE.md) | [system-overview.md](../architecture/system-overview.md) | [streams.md](../architecture/streams.md)

*Tech Lead Review — Cập nhật: 2026-05-18*

---

## 1. System Architecture Overview

```mermaid
flowchart TB
    subgraph CLIENT["Client"]
        HTTP["HTTP Client / Frontend"]
    end

    subgraph API["API Layer — api/kb/kb_api.py"]
        UP["POST /kb/upload\n(multipart + file)"]
        IN["POST /kb/ingest\n(JSON raw_content)"]
        ST["GET /kb/status/{id}"]
        DEL["DELETE /kb/{id}"]
    end

    subgraph PARSE["File Parsing — core/utils/kb_file_parser.py"]
        FP[".txt  .md  .docx\n.cfg  .conf\n.json  .xml  .pdf(MinerU)"]
    end

    subgraph MQ["RabbitMQ — default_broker"]
        QI["kb_ingest queue"]
        QE["kb_enrich queue"]
    end

    subgraph CONSUMER["FastStream Consumers — streams/consumers/kb.py"]
        CI["consumer_kb_ingest"]
        CE["consumer_kb_enrich"]
    end

    subgraph FACTORY["Service Factory — core/services/"]
        FACT["KBServiceFactory\nregistry: doc_type → service"]
    end

    subgraph SVC["Service Implementations"]
        WI["WorkInstructionKBService\nUS2.3 ✅"]
        VC["VendorConfigKBService\nUS2.4 ✅"]
        TS["TicketSummaryKBService\nUS2.2 ⚠️"]
        TA["TicketActivityKBService\nUS2.2 ⚠️"]
        TX["TextKBService\nfallback"]
    end

    subgraph CHUNK["Chunking — core/utils/"]
        CKT["kb_chunker.py\nSliding window 512 tok\nSentence boundary"]
        CKV["kb_vendor_config_chunker.py\nSection-aware split\nUS2.4"]
        VCP["kb_vendor_config_parser.py\niOS CLI / JunOS / JSON / XML"]
    end

    subgraph EMBED["Embedding — core/utils/kb_embedder.py (US2.1) ✅"]
        EMB["gemini-embedding-001\n768-dim (Matryoshka)\nOpenAI text-embedding-3-small (fallback)"]
    end

    subgraph STORAGE["Storage"]
        MONGO[("MongoDB\nKBDocuments\nKBChunks")]
        PG[("PostgreSQL + pgvector\nkb_vectors\nHNSW cosine index\ndim=768")]
        REDIS[("Redis\nCDC cursor\nkb:sync_cursor:{tenant}:{kb}")]
    end

    subgraph BATCH["US2.2 — TaskIQ Batch (tasks/handlers/kb.py)"]
        BTASK["kb_batch_ingest_handler\nCDC incremental"]
        STUB["⚠️ TicketORM STUB\nBlocked: US4.2 TrongND33"]
    end

    subgraph FUTURE["US2.5 — Query/RAG ⚠️ NOT IMPLEMENTED"]
        QSTUB["BaseKBService.query() = ...\nNeeds: vector_search + reranker + LLM"]
    end

    HTTP --> UP & IN & ST & DEL
    UP --> FP --> QI
    IN --> QI
    DEL & ST --> FACT

    QI --> CI --> FACT
    FACT --> WI & VC & TS & TA & TX

    WI --> CKT
    VC --> CKV --> VCP

    WI & VC & TS & TA & TX --> MONGO
    WI & VC & TS & TA & TX --> QE

    QE --> CE --> EMBED --> EMB --> PG

    BTASK -.->|blocked| STUB
    BTASK --> REDIS
    BTASK --> FACT
```

---

## 2. Ingest Pipeline — Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as POST /kb/upload
    participant FP as kb_file_parser
    participant MQ as RabbitMQ
    participant CI as consumer_kb_ingest
    participant SVC as KB*Service.ingest()
    participant MDB as MongoDB
    participant CE as consumer_kb_enrich
    participant EMB as kb_embedder
    participant PG as pgvector

    C->>API: POST multipart (file + metadata)
    API->>FP: parse_file_to_text(tmp_file)
    FP-->>API: raw_content: str
    API->>MQ: publish kb_ingest message
    API-->>C: {success: true, status: "queued", document_id}

    MQ->>CI: consume kb_ingest
    CI->>SVC: KBServiceFactory.get(doc_type).ingest(...)

    SVC->>MDB: KBDocumentModel.upsert() → status=processing
    SVC->>MDB: KBChunks.delete_many() — clear stale chunks
    SVC->>SVC: _make_chunker_fn(extra_metadata)(raw_content) → raw_chunks[]

    loop For each raw_chunk
        SVC->>SVC: enrich_chunk(chunk, message) — populate metadata
        SVC->>MDB: KBChunkModel.commit()
    end

    SVC->>MDB: KBDocumentModel.commit() → status=done, chunk_count=N
    SVC->>MQ: publish kb_enrich {tenant_id, kb_id, document_id}

    MQ->>CE: consume kb_enrich
    CE->>MDB: KBChunks.find({document_id}) → chunks[]
    CE->>EMB: embed_batch(content_clean[]) → vectors[]
    Note over EMB: gemini-embedding-001<br/>768-dim Matryoshka<br/>batch ≤ 100

    loop For each (chunk, vector)
        CE->>PG: upsert_vector(chunk_id, embedding, metadata)
    end
```

---

## 3. Service Layer — Class Diagram

```mermaid
classDiagram
    class BaseKBService {
        <<abstract>>
        +DOC_TYPE: str
        +get_chunker()* Callable
        +enrich_chunk(chunk, message)*
        +_extra_chunk_metadata(raw) dict
        +_make_chunker_fn(extra_metadata) Callable
        +ingest(tenant_id, kb_id, document_id, ...) KBIngestResult
        +query(tenant_id, kb_id, question, ...) KBQueryResult
        +delete(tenant_id, document_id)
        +get_status(tenant_id, document_id) dict
    }

    class WorkInstructionKBService {
        +DOC_TYPE = "work_instruction"
        +get_chunker() chunk_text
        +enrich_chunk()
        metadata: category, version, tags
        file_name, file_type, doc_type, source
    }

    class VendorConfigKBService {
        +DOC_TYPE = "vendor_config"
        +get_chunker() chunk_vendor_config
        +_make_chunker_fn(extra) partial~config_type~
        +_extra_chunk_metadata(raw) section fields
        +enrich_chunk()
        metadata: vendor, model, firmware_version
        hostname, config_type, section_type
        section_name, section_path
    }

    class TicketKBService {
        +DOC_TYPE = "ticket_summary"
        +get_chunker() _single_chunk
        +enrich_chunk()
        metadata: ticket_id, priority, queue
    }

    class TicketSummaryKBService {
        +DOC_TYPE = "ticket_summary"
        +assemble(ticket)$ str
        metadata: cus_type, issue_id, status
    }

    class TicketActivityKBService {
        +DOC_TYPE = "ticket_activity"
        +assemble(activity)$ str
        metadata: step_name, action_name
    }

    class TextKBService {
        +DOC_TYPE = "text"
        +get_chunker() chunk_text
        minimal metadata
    }

    class KBServiceFactory {
        -_REGISTRY dict
        +get(doc_type)$ BaseKBService
        +register(doc_type, cls)$
        +supported_types()$ list
    }

    BaseKBService <|-- WorkInstructionKBService : US2.3 ✅
    BaseKBService <|-- VendorConfigKBService : US2.4 ✅
    BaseKBService <|-- TicketKBService : US2.2 ⚠️
    BaseKBService <|-- TextKBService
    TicketKBService <|-- TicketSummaryKBService
    TicketKBService <|-- TicketActivityKBService
    KBServiceFactory --> BaseKBService : creates
```

---

## 4. Chunking Strategy — Decision Tree

```mermaid
flowchart LR
    INPUT["raw_content\ndoc_type\nextra_metadata"]

    INPUT --> DT{doc_type?}

    DT -->|work_instruction| WI_C["kb_chunker.chunk_text\nchunk_size=512 tok\noverlap=64 tok\nSentence boundary aware"]
    DT -->|vendor_config| VC_DT{config_type?}
    DT -->|ticket_*| TK_C["_single_chunk\nEntire ticket = 1 chunk\nNo split"]
    DT -->|text| TX_C["kb_chunker.chunk_text\nDefault params"]

    VC_DT -->|ios_cli| IOS["Parse by ! delimiter\nSection type: interface\nrouting / acl / vlan / global"]
    VC_DT -->|junos_cli| JUN["Parse by brace depth\nSection: interfaces\nrouting-options / firewall"]
    VC_DT -->|json| JSON["Split by top-level keys\nFallback: single chunk"]
    VC_DT -->|xml| XML["Split by root children\nStrip namespace URI"]
    VC_DT -->|auto| AUTO["detect_config_type()\nxml → json → junos → ios"]

    IOS & JUN & JSON & XML --> SIZE{section > 1000 chars?}
    SIZE -->|yes| SLIDE["Fallback: chunk_text\nsame sliding window"]
    SIZE -->|no| SINGLE["1 section = 1 chunk\nPreserves context"]
```

---

## 5. Data Models

```mermaid
erDiagram
    KBDocuments {
        ObjectId _id PK
        string tenant_id
        string kb_id
        string document_id UK
        string title
        string doc_type "work_instruction|vendor_config|ticket_summary|ticket_activity|text"
        string source_type "file|text|db"
        string source_url
        string status "queued|processing|done|failed"
        int chunk_count
        string error
        dict extra_metadata
        datetime created_at
        datetime updated_at
    }

    KBChunks {
        ObjectId _id PK
        string tenant_id
        string kb_id
        string document_id FK
        int chunk_index
        string content
        string content_clean
        int char_start
        int char_end
        dict metadata "doc_type, source, category, vendor, section_type, ..."
        datetime created_at
    }

    kb_vectors {
        UUID id PK
        varchar tenant_id
        varchar kb_id
        varchar document_id FK
        varchar chunk_id UK "document_id_chunkIndex"
        text content
        jsonb metadata
        vector embedding "dim=768 HNSW cosine"
        timestamptz created_at
    }

    redis_sync_cursor {
        string key "kb:sync_cursor:{tenant_id}:{kb_id}"
        string value "ISO 8601 datetime"
    }

    KBDocuments ||--o{ KBChunks : "document_id"
    KBChunks ||--o| kb_vectors : "chunk_id"
    redis_sync_cursor }o--|| KBDocuments : "tracks last ingest time"
```

---

## 6. US Status Summary

| US | Tên | Components chính | Status | Blocker |
|----|-----|-----------------|--------|---------|
| **US2.1** | Embedding Pipeline | `kb_embedder.py` · `kb_vectors.py` · `pgvector_client.py` · `kb_enrich_handler` | ✅ Done | — |
| **US2.2** | Ticket Batch Ingest | `kb_batch_ingest_handler` · `kb_sync_cursor.py` · `TicketSummaryKBService` | ⚠️ Stubbed | `TicketORM` — US4.2 (TrongND33) |
| **US2.3** | Work Instruction KB | `WorkInstructionKBService` · `kb_chunker.py` · `kb_file_parser.py` | ✅ Done | — |
| **US2.4** | Vendor Config KB | `VendorConfigKBService` · `kb_vendor_config_chunker.py` · `kb_vendor_config_parser.py` | ✅ Done | — |
| **US2.5** | Query / RAG | `BaseKBService.query()` · `vector_search()` | ❌ Not impl | Reranker + LLM (next sprint) |

---

## 7. Infrastructure Dependencies

```mermaid
flowchart LR
    subgraph APP["agenticai service\nport 5018"]
        API2["FastAPI\nuvicorn 2 workers"]
        CONS["FastStream consumer\nrbmq-default-consumer"]
    end

    subgraph INFRA["172.27.230.30"]
        RBMQ["RabbitMQ :5672\nvhost: multiagents"]
        MDB2["MongoDB :27017\nDB: omnichannel"]
        PG2["PostgreSQL :15432\nndd_postgres_master\nExtension: pgvector 0.8.2"]
        RDS["Redis :6379\nDB 10"]
    end

    subgraph EXTERNAL["External APIs"]
        GEMINI["Google AI\ngemini-embedding-001\nv1 API"]
        OPENAI["FPT Cloud OpenAI\ntext-embedding-3-small\n(fallback)"]
    end

    API2 -->|publish kb_ingest| RBMQ
    API2 -->|publish kb_enrich| RBMQ
    RBMQ -->|consume| CONS
    API2 --> MDB2
    CONS --> MDB2
    CONS --> PG2
    API2 --> RDS
    CONS --> GEMINI
    CONS -.->|fallback| OPENAI
```
