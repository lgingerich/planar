# Multi-Table Transactions

## Purpose

This document specifies Planar's approach to atomic transactions spanning multiple tables. Multi-table transactions enable consistent updates across related tables, supporting use cases like star schema updates, cross-table integrity constraints, and coordinated data pipelines.

## Motivation

Single-table transactions are sufficient for many workloads, but some scenarios require atomicity across tables:

1. **Star schema updates**: Fact and dimension tables must be updated together to maintain referential integrity.

2. **Data pipelines**: ETL jobs that write to multiple destination tables need atomic commits to avoid partial failures.

3. **Table migrations**: Moving data from one table to another (e.g., schema changes, partitioning changes) should be atomic.

4. **Cross-table constraints**: Business rules that span tables (e.g., "total orders must equal sum of line items") require coordinated updates.

5. **Audit consistency**: Changes to data tables and audit tables should commit together.

Without multi-table transactions, applications must implement complex compensating logic to handle partial failures, or accept inconsistency windows.

## Design Principles

1. **Opt-in complexity**: Multi-table transactions add overhead. Single-table transactions remain the default and optimized path.

2. **Serializable isolation**: Multi-table transactions provide serializable isolation across participating tables.

3. **Deadlock prevention**: The protocol prevents deadlocks through consistent ordering.

4. **Failure recovery**: Prepared transactions can be recovered after coordinator failure.

5. **Minimal lock duration**: Locks are held only during the commit phase, not during data upload.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Transaction Coordinator                             │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Multi-Table Transaction                           │   │
│  │                                                                     │   │
│  │   Table A mutations ─┬─> Prepare A ─┐                               │   │
│  │   Table B mutations ─┼─> Prepare B ─┼─> All prepared? ─> Commit All │   │
│  │   Table C mutations ─┴─> Prepare C ─┘         │                     │   │
│  │                                               │                     │   │
│  │                                         Any failed? ─> Abort All    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Coordinates
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (DB)                             │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │    Table A      │    │    Table B      │    │    Table C      │         │
│  │                 │    │                 │    │                 │         │
│  │ current_txn_id  │    │ current_txn_id  │    │ current_txn_id  │         │
│  │ (locked)        │    │ (locked)        │    │ (locked)        │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    prepared_transactions                             │   │
│  │  global_txn_id | table_uuid | local_txn_id | state | prepared_at    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Two-Phase Commit Protocol

### Overview

Planar uses a two-phase commit (2PC) protocol:

1. **Phase 1 (Prepare)**: Each participating table prepares its changes. Changes are validated and locks acquired, but `current_transaction_id` is not yet advanced.

2. **Phase 2 (Commit/Abort)**: If all tables prepare successfully, all are committed atomically. If any fails, all are aborted.

### Transaction States

```rust
/// State of a multi-table transaction
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultiTableTxnState {
    /// Transaction started, collecting mutations
    Active,
    /// Prepare phase initiated
    Preparing,
    /// All participants prepared successfully
    Prepared,
    /// Commit phase initiated
    Committing,
    /// Transaction committed successfully
    Committed,
    /// Abort initiated
    Aborting,
    /// Transaction aborted
    Aborted,
}

/// A participant in a multi-table transaction
#[derive(Clone, Debug)]
pub struct Participant {
    /// Table UUID
    pub table_uuid: Uuid,
    /// Base transaction ID (what we read)
    pub base_transaction_id: Uuid,
    /// Prepared transaction ID (what we'll commit)
    pub prepared_transaction_id: Option<Uuid>,
    /// Mutations for this table
    pub mutations: Vec<MutationOp>,
    /// Participant state
    pub state: ParticipantState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Pending,
    Prepared,
    Committed,
    Aborted,
}
```

### Protocol Flow

```
┌────────────┐         ┌─────────────┐         ┌─────────────┐
│ Coordinator│         │  Table A    │         │  Table B    │
└─────┬──────┘         └──────┬──────┘         └──────┬──────┘
      │                       │                       │
      │  1. Begin Multi-Txn   │                       │
      │───────────────────────│───────────────────────│
      │                       │                       │
      │  2. Add mutations     │                       │
      │       for Table A     │                       │
      │───────────────────────>                       │
      │                       │                       │
      │  3. Add mutations     │                       │
      │       for Table B     │                       │
      │───────────────────────│───────────────────────>
      │                       │                       │
      │  4. Upload files to   │                       │
      │     object storage    │                       │
      │───────────────────────│───────────────────────│
      │                       │                       │
      │  ========= PREPARE PHASE =========            │
      │                       │                       │
      │  5. PREPARE Table A   │                       │
      │───────────────────────>                       │
      │       Lock table      │                       │
      │       Validate base   │                       │
      │       Insert txn row  │                       │
      │       Insert file rows│                       │
      │   <───────────────────│                       │
      │       PREPARED        │                       │
      │                       │                       │
      │  6. PREPARE Table B   │                       │
      │───────────────────────│───────────────────────>
      │                       │       Lock table      │
      │                       │       Validate base   │
      │                       │       Insert txn row  │
      │                       │       Insert file rows│
      │   <───────────────────│───────────────────────│
      │       PREPARED        │                       │
      │                       │                       │
      │  ========= COMMIT PHASE =========             │
      │                       │                       │
      │  7. COMMIT Table A    │                       │
      │───────────────────────>                       │
      │       Update current  │                       │
      │       Release lock    │                       │
      │   <───────────────────│                       │
      │       COMMITTED       │                       │
      │                       │                       │
      │  8. COMMIT Table B    │                       │
      │───────────────────────│───────────────────────>
      │                       │       Update current  │
      │                       │       Release lock    │
      │   <───────────────────│───────────────────────│
      │       COMMITTED       │                       │
      │                       │                       │
```

### Abort Flow

If any prepare fails:

```
      │  5. PREPARE Table A   │                       │
      │───────────────────────>                       │
      │   <───────────────────│                       │
      │       PREPARED        │                       │
      │                       │                       │
      │  6. PREPARE Table B   │                       │
      │───────────────────────│───────────────────────>
      │   <───────────────────│───────────────────────│
      │       FAILED          │                       │
      │       (conflict)      │                       │
      │                       │                       │
      │  ========= ABORT PHASE =========              │
      │                       │                       │
      │  7. ABORT Table A     │                       │
      │───────────────────────>                       │
      │       Delete txn row  │                       │
      │       Delete file rows│                       │
      │       Release lock    │                       │
      │   <───────────────────│                       │
      │       ABORTED         │                       │
      │                       │                       │
```

## Implementation

### Schema Changes

```sql
-- Global transaction tracking
CREATE TABLE IF NOT EXISTS global_transactions (
    global_txn_id BLOB PRIMARY KEY,
    state TEXT NOT NULL, -- 'preparing', 'prepared', 'committing', 'committed', 'aborting', 'aborted'
    coordinator_id TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    prepared_at TIMESTAMP,
    committed_at TIMESTAMP,
    aborted_at TIMESTAMP,
    timeout_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_global_txn_state
    ON global_transactions(state, timeout_at);

-- Participant tracking
CREATE TABLE IF NOT EXISTS transaction_participants (
    global_txn_id BLOB NOT NULL,
    table_uuid BLOB NOT NULL,
    base_transaction_id BLOB NOT NULL,
    prepared_transaction_id BLOB,
    state TEXT NOT NULL, -- 'pending', 'prepared', 'committed', 'aborted'
    prepared_at TIMESTAMP,
    PRIMARY KEY (global_txn_id, table_uuid),
    FOREIGN KEY (global_txn_id) REFERENCES global_transactions(global_txn_id),
    FOREIGN KEY (table_uuid) REFERENCES tables(table_uuid)
);

CREATE INDEX IF NOT EXISTS idx_participants_table
    ON transaction_participants(table_uuid, state);
```

### Coordinator Implementation

```rust
/// Multi-table transaction coordinator
pub struct MultiTableTransaction {
    /// Global transaction ID
    pub global_txn_id: Uuid,
    /// Participating tables
    pub participants: HashMap<Uuid, Participant>,
    /// Transaction state
    pub state: MultiTableTxnState,
    /// Coordinator ID (for recovery)
    pub coordinator_id: String,
    /// Timeout
    pub timeout: DateTime<Utc>,
}

impl MultiTableTransaction {
    /// Create a new multi-table transaction
    pub async fn begin(
        catalog: &SqlCatalog,
        timeout: Duration,
    ) -> Result<Self, CatalogError> {
        let global_txn_id = Uuid::new_v4();
        let coordinator_id = catalog.coordinator_id().to_string();
        let timeout_at = Utc::now() + timeout;
        
        // Record global transaction
        catalog.create_global_transaction(
            global_txn_id,
            &coordinator_id,
            timeout_at,
        ).await?;
        
        Ok(Self {
            global_txn_id,
            participants: HashMap::new(),
            state: MultiTableTxnState::Active,
            coordinator_id,
            timeout: timeout_at,
        })
    }
    
    /// Add mutations for a table
    pub async fn add_mutations(
        &mut self,
        catalog: &SqlCatalog,
        table_uuid: Uuid,
        base_transaction_id: Uuid,
        mutations: Vec<MutationOp>,
    ) -> Result<(), CatalogError> {
        if self.state != MultiTableTxnState::Active {
            return Err(CatalogError::InvalidTransactionState(
                "Cannot add mutations after prepare".into()
            ));
        }
        
        // Record participant
        let participant = Participant {
            table_uuid,
            base_transaction_id,
            prepared_transaction_id: None,
            mutations,
            state: ParticipantState::Pending,
        };
        
        self.participants.insert(table_uuid, participant);
        
        catalog.add_transaction_participant(
            self.global_txn_id,
            table_uuid,
            base_transaction_id,
        ).await?;
        
        Ok(())
    }
    
    /// Execute prepare phase for all participants
    pub async fn prepare(
        &mut self,
        catalog: &SqlCatalog,
    ) -> Result<(), CatalogError> {
        self.state = MultiTableTxnState::Preparing;
        catalog.update_global_transaction_state(
            self.global_txn_id,
            "preparing",
        ).await?;
        
        // Sort tables by UUID for consistent ordering (deadlock prevention)
        let mut table_order: Vec<Uuid> = self.participants.keys().cloned().collect();
        table_order.sort();
        
        // Prepare each participant in order
        for table_uuid in &table_order {
            let participant = self.participants.get_mut(table_uuid).unwrap();
            
            match catalog.prepare_table(
                self.global_txn_id,
                *table_uuid,
                participant.base_transaction_id,
                &participant.mutations,
            ).await {
                Ok(prepared_txn_id) => {
                    participant.prepared_transaction_id = Some(prepared_txn_id);
                    participant.state = ParticipantState::Prepared;
                    
                    catalog.update_participant_state(
                        self.global_txn_id,
                        *table_uuid,
                        "prepared",
                        Some(prepared_txn_id),
                    ).await?;
                }
                Err(e) => {
                    // Prepare failed, need to abort all prepared participants
                    self.abort(catalog).await?;
                    return Err(e);
                }
            }
        }
        
        self.state = MultiTableTxnState::Prepared;
        catalog.update_global_transaction_state(
            self.global_txn_id,
            "prepared",
        ).await?;
        
        Ok(())
    }
    
    /// Execute commit phase for all participants
    pub async fn commit(
        &mut self,
        catalog: &SqlCatalog,
    ) -> Result<(), CatalogError> {
        if self.state != MultiTableTxnState::Prepared {
            return Err(CatalogError::InvalidTransactionState(
                "Must prepare before commit".into()
            ));
        }
        
        self.state = MultiTableTxnState::Committing;
        catalog.update_global_transaction_state(
            self.global_txn_id,
            "committing",
        ).await?;
        
        // Commit each participant
        // Note: Once committing starts, we must complete (no partial commit allowed)
        for (table_uuid, participant) in &mut self.participants {
            let prepared_txn_id = participant.prepared_transaction_id
                .ok_or_else(|| CatalogError::InvalidTransactionState(
                    "Participant not prepared".into()
                ))?;
            
            catalog.commit_prepared(
                self.global_txn_id,
                *table_uuid,
                prepared_txn_id,
            ).await?;
            
            participant.state = ParticipantState::Committed;
            
            catalog.update_participant_state(
                self.global_txn_id,
                *table_uuid,
                "committed",
                None,
            ).await?;
        }
        
        self.state = MultiTableTxnState::Committed;
        catalog.update_global_transaction_state(
            self.global_txn_id,
            "committed",
        ).await?;
        
        Ok(())
    }
    
    /// Abort the transaction
    pub async fn abort(
        &mut self,
        catalog: &SqlCatalog,
    ) -> Result<(), CatalogError> {
        self.state = MultiTableTxnState::Aborting;
        catalog.update_global_transaction_state(
            self.global_txn_id,
            "aborting",
        ).await?;
        
        // Abort each prepared participant
        for (table_uuid, participant) in &mut self.participants {
            if participant.state == ParticipantState::Prepared {
                catalog.abort_prepared(
                    self.global_txn_id,
                    *table_uuid,
                    participant.prepared_transaction_id.unwrap(),
                ).await?;
            }
            
            participant.state = ParticipantState::Aborted;
            
            catalog.update_participant_state(
                self.global_txn_id,
                *table_uuid,
                "aborted",
                None,
            ).await?;
        }
        
        self.state = MultiTableTxnState::Aborted;
        catalog.update_global_transaction_state(
            self.global_txn_id,
            "aborted",
        ).await?;
        
        Ok(())
    }
}
```

### Catalog Methods for 2PC

```rust
impl SqlCatalog {
    /// Prepare a table for commit (Phase 1)
    pub async fn prepare_table(
        &self,
        global_txn_id: Uuid,
        table_uuid: Uuid,
        base_transaction_id: Uuid,
        mutations: &[MutationOp],
    ) -> Result<Uuid, CatalogError> {
        let mut conn = self.pool.acquire().await?;
        
        // Begin database transaction
        let mut db_txn = conn.begin().await?;
        
        // Lock the table row (SELECT FOR UPDATE)
        let table = sqlx::query_as::<_, Table>(
            "SELECT * FROM tables WHERE table_uuid = ? FOR UPDATE"
        )
        .bind(table_uuid.as_bytes().as_slice())
        .fetch_one(&mut *db_txn)
        .await?;
        
        // Validate base transaction
        let current_transaction_id = table.current_transaction_id
            .ok_or_else(|| CatalogError::NoTransaction)?;
        
        if current_transaction_id != base_transaction_id {
            return Err(CatalogError::Conflict(format!(
                "Base transaction {} does not match current {}",
                base_transaction_id, current_transaction_id
            )));
        }
        
        // Create new transaction record
        let new_txn_id = Uuid::new_v4();
        let now = Utc::now();
        
        sqlx::query(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
             VALUES (?, ?, ?, ?)"
        )
        .bind(new_txn_id.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .bind(now)
        .bind(base_transaction_id.as_bytes().as_slice())
        .execute(&mut *db_txn)
        .await?;
        
        // Apply mutations (insert file rows, etc.)
        for mutation in mutations {
            self.apply_mutation(&mut db_txn, table_uuid, new_txn_id, mutation).await?;
        }
        
        // DO NOT update current_transaction_id yet
        // That happens in Phase 2 (commit)
        
        // Commit database transaction (but table is still "uncommitted")
        db_txn.commit().await?;
        
        Ok(new_txn_id)
    }
    
    /// Commit a prepared transaction (Phase 2)
    pub async fn commit_prepared(
        &self,
        global_txn_id: Uuid,
        table_uuid: Uuid,
        prepared_txn_id: Uuid,
    ) -> Result<(), CatalogError> {
        let mut conn = self.pool.acquire().await?;
        let mut db_txn = conn.begin().await?;
        
        // Lock table row
        let _table = sqlx::query_as::<_, Table>(
            "SELECT * FROM tables WHERE table_uuid = ? FOR UPDATE"
        )
        .bind(table_uuid.as_bytes().as_slice())
        .fetch_one(&mut *db_txn)
        .await?;
        
        // Update current_transaction_id to the prepared transaction
        sqlx::query(
            "UPDATE tables SET current_transaction_id = ? WHERE table_uuid = ?"
        )
        .bind(prepared_txn_id.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .execute(&mut *db_txn)
        .await?;
        
        db_txn.commit().await?;
        
        Ok(())
    }
    
    /// Abort a prepared transaction
    pub async fn abort_prepared(
        &self,
        global_txn_id: Uuid,
        table_uuid: Uuid,
        prepared_txn_id: Uuid,
    ) -> Result<(), CatalogError> {
        let mut conn = self.pool.acquire().await?;
        let mut db_txn = conn.begin().await?;
        
        // Delete the prepared transaction and its files
        // (They were never made visible by updating current_transaction_id)
        
        sqlx::query(
            "DELETE FROM files WHERE added_in_transaction_id = ?"
        )
        .bind(prepared_txn_id.as_bytes().as_slice())
        .execute(&mut *db_txn)
        .await?;
        
        sqlx::query(
            "DELETE FROM transactions WHERE transaction_id = ?"
        )
        .bind(prepared_txn_id.as_bytes().as_slice())
        .execute(&mut *db_txn)
        .await?;
        
        db_txn.commit().await?;
        
        Ok(())
    }
}
```

## Deadlock Prevention

Deadlocks are prevented through consistent table ordering:

```rust
impl MultiTableTransaction {
    /// Get tables in lock order
    fn lock_order(participants: &HashMap<Uuid, Participant>) -> Vec<Uuid> {
        let mut tables: Vec<Uuid> = participants.keys().cloned().collect();
        tables.sort(); // Consistent ordering by UUID
        tables
    }
}
```

All coordinators acquire locks in the same order, preventing circular wait conditions.

## Recovery

### Coordinator Failure Recovery

If the coordinator crashes, orphaned transactions must be cleaned up:

```rust
pub struct TransactionRecovery {
    catalog: SqlCatalog,
    coordinator_id: String,
}

impl TransactionRecovery {
    /// Recover orphaned transactions
    pub async fn recover(&self) -> Result<RecoveryStats, CatalogError> {
        let mut stats = RecoveryStats::default();
        
        // Find transactions in intermediate states that have timed out
        let orphaned = self.catalog.find_orphaned_transactions(
            self.coordinator_id.clone(),
            Utc::now(),
        ).await?;
        
        for global_txn in orphaned {
            match global_txn.state.as_str() {
                "preparing" => {
                    // Preparing: safe to abort (no commits have happened)
                    self.abort_transaction(global_txn.global_txn_id).await?;
                    stats.aborted += 1;
                }
                
                "prepared" => {
                    // Prepared but not committed: safe to abort
                    self.abort_transaction(global_txn.global_txn_id).await?;
                    stats.aborted += 1;
                }
                
                "committing" => {
                    // Committing: must complete the commit
                    // Some participants may already be committed
                    self.complete_commit(global_txn.global_txn_id).await?;
                    stats.committed += 1;
                }
                
                "aborting" => {
                    // Aborting: complete the abort
                    self.complete_abort(global_txn.global_txn_id).await?;
                    stats.aborted += 1;
                }
                
                _ => {
                    // 'active', 'committed', 'aborted' don't need recovery
                }
            }
        }
        
        Ok(stats)
    }
    
    async fn complete_commit(&self, global_txn_id: Uuid) -> Result<(), CatalogError> {
        let participants = self.catalog.get_transaction_participants(global_txn_id).await?;
        
        for participant in participants {
            if participant.state != "committed" {
                self.catalog.commit_prepared(
                    global_txn_id,
                    participant.table_uuid,
                    participant.prepared_transaction_id.unwrap(),
                ).await?;
                
                self.catalog.update_participant_state(
                    global_txn_id,
                    participant.table_uuid,
                    "committed",
                    None,
                ).await?;
            }
        }
        
        self.catalog.update_global_transaction_state(
            global_txn_id,
            "committed",
        ).await?;
        
        Ok(())
    }
    
    async fn complete_abort(&self, global_txn_id: Uuid) -> Result<(), CatalogError> {
        let participants = self.catalog.get_transaction_participants(global_txn_id).await?;
        
        for participant in participants {
            if participant.state == "prepared" {
                self.catalog.abort_prepared(
                    global_txn_id,
                    participant.table_uuid,
                    participant.prepared_transaction_id.unwrap(),
                ).await?;
            }
            
            self.catalog.update_participant_state(
                global_txn_id,
                participant.table_uuid,
                "aborted",
                None,
            ).await?;
        }
        
        self.catalog.update_global_transaction_state(
            global_txn_id,
            "aborted",
        ).await?;
        
        Ok(())
    }
}

/// Background recovery worker
pub async fn recovery_worker(catalog: SqlCatalog, coordinator_id: String) {
    let recovery = TransactionRecovery { catalog, coordinator_id };
    
    loop {
        if let Err(e) = recovery.recover().await {
            log::error!("Transaction recovery failed: {}", e);
        }
        
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
```

### Participant Failure Recovery

If a participant table becomes unavailable during commit:

```rust
impl SqlCatalog {
    /// Commit with retry for transient failures
    pub async fn commit_prepared_with_retry(
        &self,
        global_txn_id: Uuid,
        table_uuid: Uuid,
        prepared_txn_id: Uuid,
        max_retries: usize,
    ) -> Result<(), CatalogError> {
        let mut retries = 0;
        
        loop {
            match self.commit_prepared(global_txn_id, table_uuid, prepared_txn_id).await {
                Ok(()) => return Ok(()),
                Err(CatalogError::Database(e)) if is_transient(&e) && retries < max_retries => {
                    retries += 1;
                    let delay = Duration::from_millis(100 * 2u64.pow(retries as u32));
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
```

## API Usage

### Example: Star Schema Update

```rust
// Update fact and dimension tables atomically
async fn update_star_schema(
    catalog: &SqlCatalog,
    fact_table: Uuid,
    dim_table: Uuid,
    fact_data: RecordBatch,
    dim_data: RecordBatch,
) -> Result<(), CatalogError> {
    // Begin multi-table transaction
    let mut txn = MultiTableTransaction::begin(
        catalog,
        Duration::from_secs(300), // 5 minute timeout
    ).await?;
    
    // Get base transactions
    let fact_base = catalog.get_current_transaction(fact_table).await?;
    let dim_base = catalog.get_current_transaction(dim_table).await?;
    
    // Upload files
    let fact_file = upload_file(&fact_data, fact_table).await?;
    let dim_file = upload_file(&dim_data, dim_table).await?;
    
    // Add mutations
    txn.add_mutations(catalog, fact_table, fact_base, vec![
        MutationOp::AddFile(fact_file),
    ]).await?;
    
    txn.add_mutations(catalog, dim_table, dim_base, vec![
        MutationOp::AddFile(dim_file),
    ]).await?;
    
    // Execute 2PC
    txn.prepare(catalog).await?;
    txn.commit(catalog).await?;
    
    Ok(())
}
```

### Example: Table Migration

```rust
// Move data from old table to new table atomically
async fn migrate_table(
    catalog: &SqlCatalog,
    old_table: Uuid,
    new_table: Uuid,
) -> Result<(), CatalogError> {
    let mut txn = MultiTableTransaction::begin(
        catalog,
        Duration::from_secs(3600), // 1 hour timeout for large migration
    ).await?;
    
    // Get current states
    let old_base = catalog.get_current_transaction(old_table).await?;
    let new_base = catalog.get_current_transaction(new_table).await?;
    
    // Read all data from old table
    let files = catalog.list_files_at(old_table, old_base).await?;
    
    // Copy/transform to new table format
    let new_files = transform_files(&files).await?;
    
    // Add mutations
    // New table: add all new files
    txn.add_mutations(catalog, new_table, new_base, 
        new_files.iter().map(|f| MutationOp::AddFile(f.clone())).collect()
    ).await?;
    
    // Old table: mark all files as removed
    txn.add_mutations(catalog, old_table, old_base,
        files.iter().map(|f| MutationOp::RemoveFile { file_uuid: f.file_uuid }).collect()
    ).await?;
    
    // Execute 2PC
    txn.prepare(catalog).await?;
    txn.commit(catalog).await?;
    
    Ok(())
}
```

## Limitations

### Performance Impact

Multi-table transactions have higher overhead than single-table:

| Aspect | Single-Table | Multi-Table |
|--------|--------------|-------------|
| Database round-trips | 1 | 2-3 per table |
| Lock duration | Short (commit only) | Longer (prepare to commit) |
| Conflict probability | Per-table | Combined |
| Recovery complexity | None | Coordinator recovery |

### Known Limitations

1. **No cross-database transactions**: All tables must be in the same catalog database.

2. **Lock escalation**: Each participating table is fully locked during prepare-to-commit phase.

3. **Timeout sensitivity**: Long-running uploads between prepare and commit can cause timeouts.

4. **No savepoints**: Cannot partially commit a multi-table transaction.

## Alternatives Considered

### Saga Pattern

Instead of 2PC, use compensating transactions:

**Pros**:
- No distributed locks
- Each step commits independently
- Better for long-running operations

**Cons**:
- Eventual consistency (not atomic)
- Must implement compensation logic
- Complex error handling

**Decision**: 2PC chosen for strong consistency guarantees. Saga can be added later for specific use cases.

### Logical Timestamps

Use logical timestamps instead of locks:

**Pros**:
- No blocking
- Better concurrency

**Cons**:
- Complex ordering logic
- Harder to reason about
- May require retry on conflict

**Decision**: Lock-based 2PC is simpler and sufficient for expected workloads.

## Implementation Phases

### Phase 1: Basic 2PC

1. Add global transaction tables
2. Implement prepare/commit/abort methods
3. Implement coordinator
4. Add deadlock prevention

### Phase 2: Recovery

1. Implement recovery worker
2. Add timeout handling
3. Add commit-with-retry logic
4. Test failure scenarios

### Phase 3: Optimization

1. Add batch prepare for efficiency
2. Implement lock timeouts
3. Add metrics and monitoring
4. Optimize lock contention

## Testing Strategy

### Unit Tests

- Transaction state machine
- Deadlock prevention (ordering)
- Prepare/commit/abort logic
- Recovery state resolution

### Integration Tests

- End-to-end multi-table commit
- Concurrent multi-table transactions
- Failure during each phase
- Recovery after coordinator crash

### Chaos Tests

- Random coordinator failures
- Network partitions
- Database failures during 2PC
- Timeout behavior

## Open Questions

1. **Nested transactions**: Should we support transactions within transactions?

2. **Read-your-writes**: Can a transaction see its uncommitted changes before commit?

3. **Lock granularity**: Should we support partition-level locks for better concurrency?

4. **Distributed coordination**: How do we handle multi-table transactions across multiple catalogs?

5. **Transaction metadata**: Should transactions carry user-defined metadata (e.g., for audit)?

## References

- [Two-Phase Commit Protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
- [PostgreSQL Two-Phase Commit](https://www.postgresql.org/docs/current/sql-prepare-transaction.html)
- [Apache Iceberg Multi-Table Transactions (proposal)](https://github.com/apache/iceberg/issues/1234)
- [Saga Pattern](https://microservices.io/patterns/data/saga.html)
- [db_control_plane.md](db_control_plane.md) - Single-table commit protocol
