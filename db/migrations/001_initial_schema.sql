-- ============================================================================
-- tables
-- ============================================================================
CREATE TABLE IF NOT EXISTS tables (
    table_uuid BLOB PRIMARY KEY,
    table_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    location TEXT NOT NULL,
    current_schema_uuid BLOB,
    current_transaction_id BLOB,
    created_at TIMESTAMP NOT NULL,
    properties TEXT,
    UNIQUE(namespace, table_name)
);

-- ============================================================================
-- transactions
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BLOB PRIMARY KEY,
    table_uuid BLOB NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    parent_transaction_id BLOB,
    FOREIGN KEY(table_uuid) REFERENCES tables(table_uuid),
    FOREIGN KEY(parent_transaction_id) REFERENCES transactions(transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_transactions_table 
    ON transactions(table_uuid);

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp 
    ON transactions(transaction_timestamp);

-- ============================================================================
-- schemas
-- ============================================================================
-- NOTE: May want to add a column for the Arrow IPC schema bytes

CREATE TABLE IF NOT EXISTS schemas (
    schema_uuid BLOB PRIMARY KEY,
    table_uuid BLOB NOT NULL,
    schema_version INTEGER NOT NULL,
    valid_from_transaction_id BLOB NOT NULL,
    valid_to_transaction_id BLOB,
    created_at TIMESTAMP NOT NULL,
    FOREIGN KEY(table_uuid) REFERENCES tables(table_uuid),
    FOREIGN KEY(valid_from_transaction_id) REFERENCES transactions(transaction_id),
    FOREIGN KEY(valid_to_transaction_id) REFERENCES transactions(transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_schemas_table 
    ON schemas(table_uuid);

CREATE INDEX IF NOT EXISTS idx_schemas_valid_range 
    ON schemas(table_uuid, valid_from_transaction_id, valid_to_transaction_id);

-- ============================================================================
-- columns
-- ============================================================================
CREATE TABLE IF NOT EXISTS columns (
    column_uuid BLOB PRIMARY KEY,
    schema_uuid BLOB NOT NULL,
    column_name TEXT NOT NULL,
    column_type BLOB NOT NULL,
    ordinal_position INTEGER NOT NULL,
    is_nullable BOOLEAN NOT NULL,
    FOREIGN KEY(schema_uuid) REFERENCES schemas(schema_uuid),
    UNIQUE(schema_uuid, column_name)
);

CREATE INDEX IF NOT EXISTS idx_columns_schema 
    ON columns(schema_uuid);

-- ============================================================================
-- files
-- ============================================================================
CREATE TABLE IF NOT EXISTS files (
    file_uuid BLOB PRIMARY KEY,
    table_uuid BLOB NOT NULL,
    file_format TEXT NOT NULL,
    file_path TEXT NOT NULL,
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    added_in_transaction_id BLOB NOT NULL,
    removed_in_transaction_id BLOB,
    partition_values TEXT,
    format_options TEXT,
    FOREIGN KEY(table_uuid) REFERENCES tables(table_uuid),
    FOREIGN KEY(added_in_transaction_id) REFERENCES transactions(transaction_id),
    FOREIGN KEY(removed_in_transaction_id) REFERENCES transactions(transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_files_table 
    ON files(table_uuid);

CREATE INDEX IF NOT EXISTS idx_files_active 
    ON files(table_uuid, added_in_transaction_id, removed_in_transaction_id);

-- ============================================================================
-- table_stats
-- ============================================================================
CREATE TABLE IF NOT EXISTS table_stats (
    table_uuid BLOB PRIMARY KEY,
    transaction_id BLOB NOT NULL,
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    file_count INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    FOREIGN KEY(table_uuid) REFERENCES tables(table_uuid),
    FOREIGN KEY(transaction_id) REFERENCES transactions(transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_table_stats_transaction 
    ON table_stats(transaction_id);

-- ============================================================================
-- file_column_stats
-- ============================================================================
CREATE TABLE IF NOT EXISTS file_column_stats (
    file_uuid BLOB,
    column_name TEXT,
    null_count BIGINT NOT NULL,
    nan_count BIGINT NOT NULL,
    min_value BLOB,
    max_value BLOB,
    distinct_count BIGINT,
    PRIMARY KEY(file_uuid, column_name),
    FOREIGN KEY(file_uuid) REFERENCES files(file_uuid)
);
