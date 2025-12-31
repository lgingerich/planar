# Overview

An open table format with a lightweight database-catalog architecture that uses transaction-based deltas instead of snapshots. Designed to eliminate metadata scaling bottlenecks, reduce maintenance burden, and excel at streaming/CDC workloads with efficient incremental updates.

# Data Model (In-Work)

## Architecture Summary

**TABLE** is the root with pointers to current state (`current_schema_uuid`, `current_transaction_id`).

**TRANSACTION** is a monotonically increasing sequence forming an immutable version chain. Represents deltas (what changed), not complete snapshots.

**SCHEMA** has transaction-bounded validity ranges (`valid_from/to_transaction_id`). Schema evolution is independent from data changes.

**FILE** represents physical data files. Tracks lifecycle (`added_in`/`removed_in_transaction_id`), format-specific metadata, partition values, and row counts.

**SNAPSHOTS don't exist.** Any point-in-time view is computed on-demand by filtering files/schema by transaction ID.


## Entity Relationship Diagram

```erDiagram
TABLE ||--o{ SCHEMA : has_versions
    TABLE ||--o{ FILE : contains
    TABLE ||--o{ TRANSACTION : has_history
    TABLE ||--o| TABLE_STATS : aggregates
    
    SCHEMA ||--o{ COLUMN : defines
    SCHEMA }o--|| TRANSACTION : valid_from
    SCHEMA }o--o| TRANSACTION : valid_to
    
    TRANSACTION }o--o| TRANSACTION : parent
    TRANSACTION ||--o{ FILE : added_in
    TRANSACTION ||--o{ FILE : removed_in
    
    FILE ||--o{ FILE_COLUMN_STATS : has_stats
    
    TABLE {
        uuid table_uuid PK
        string table_name
        string namespace
        string location
        uuid current_schema_uuid FK
        bigint current_transaction_id FK
        timestamp created_at
        json properties
    }

    TABLE_STATS {
        uuid table_uuid PK_FK
        bigint transaction_id FK
        bigint record_count
        bigint file_size_bytes
        integer file_count
        timestamp last_updated
    }

    TRANSACTION {
        bigint transaction_id PK
        uuid table_uuid FK
        timestamp transaction_timestamp
        bigint parent_transaction_id FK
    }

    SCHEMA {
        uuid schema_uuid PK
        uuid table_uuid FK
        integer schema_version
        bigint valid_from_transaction_id FK
        bigint valid_to_transaction_id FK
        timestamp created_at
    }

    COLUMN {
        uuid column_uuid PK
        uuid schema_uuid FK
        string column_name
        string column_type
        integer ordinal_position
        boolean is_nullable
    }
    
    FILE {
        uuid file_uuid PK
        uuid table_uuid FK
        string file_format
        string file_path
        bigint record_count
        bigint file_size_bytes
        bigint added_in_transaction_id FK
        bigint removed_in_transaction_id FK
        json partition_values
    }
    
    FILE_COLUMN_STATS {
        uuid file_uuid PK_FK
        string column_name PK
        bigint null_count
        bigint nan_count
        binary min_value
        binary max_value
        bigint distinct_count
    }
```

