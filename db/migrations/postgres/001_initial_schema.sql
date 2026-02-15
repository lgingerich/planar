-- ============================================================================
-- tables
-- ============================================================================
CREATE TABLE IF NOT EXISTS tables (
    table_uuid BYTEA PRIMARY KEY,
    table_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    location TEXT NOT NULL,
    current_schema_uuid BYTEA,
    current_transaction_id BYTEA,
    created_at TIMESTAMP NOT NULL,
    properties TEXT,
    min_reader_version INTEGER NOT NULL DEFAULT 1,
    min_writer_version INTEGER NOT NULL DEFAULT 1,
    UNIQUE(namespace, table_name)
);

-- ============================================================================
-- transactions
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BYTEA PRIMARY KEY,
    table_uuid BYTEA NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    parent_transaction_id BYTEA,
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
    schema_uuid BYTEA PRIMARY KEY,
    table_uuid BYTEA NOT NULL,
    schema_version INTEGER NOT NULL,
    valid_from_transaction_id BYTEA NOT NULL,
    valid_to_transaction_id BYTEA,
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
    column_uuid BYTEA PRIMARY KEY,
    schema_uuid BYTEA NOT NULL,
    column_name TEXT NOT NULL,
    column_type BYTEA NOT NULL,
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
    file_uuid BYTEA PRIMARY KEY,
    table_uuid BYTEA NOT NULL,
    file_format TEXT NOT NULL,
    file_path TEXT NOT NULL,
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    added_in_transaction_id BYTEA NOT NULL,
    removed_in_transaction_id BYTEA,
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
    table_uuid BYTEA PRIMARY KEY,
    transaction_id BYTEA NOT NULL,
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
    file_uuid BYTEA,
    column_name TEXT,
    null_count BIGINT NOT NULL,
    nan_count BIGINT NOT NULL,
    min_value BYTEA,
    max_value BYTEA,
    distinct_count BIGINT,
    PRIMARY KEY(file_uuid, column_name),
    FOREIGN KEY(file_uuid) REFERENCES files(file_uuid)
);
