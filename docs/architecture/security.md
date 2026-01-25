# Security and Access Control

## Purpose

This document specifies Planar's security architecture, covering authentication, authorization, encryption, and audit logging. Security enables multi-tenant deployments, data governance, and regulatory compliance.

## Motivation

Production deployments require robust security:

1. **Multi-tenancy**: Multiple teams or customers share a Planar deployment. Each must only access their own data.

2. **Data governance**: Organizations need to control who can read, write, or modify table schemas.

3. **Compliance**: Regulations (GDPR, HIPAA, SOC2) require access controls, encryption, and audit trails.

4. **External access**: When external engines connect directly to the database ([external_access.md](external_access.md)), access must be controlled.

5. **Defense in depth**: Multiple security layers protect against misconfigurations and attacks.

## Design Principles

1. **Least privilege**: Users receive only the permissions they need, nothing more.

2. **Defense in depth**: Multiple security layers (authentication, authorization, encryption) provide redundancy.

3. **Fail secure**: When security checks fail, deny access by default.

4. **Audit everything**: All access attempts are logged for forensics and compliance.

5. **Pluggable**: Support multiple authentication and authorization backends.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Security Gateway                                   │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌───────────────────────┐   │
│  │ Authentication  │───>│ Authorization   │───>│ Request Handler       │   │
│  │                 │    │                 │    │                       │   │
│  │ - JWT tokens    │    │ - RBAC          │    │ - Read operations     │   │
│  │ - API keys      │    │ - ABAC          │    │ - Write operations    │   │
│  │ - mTLS certs    │    │ - Row-level     │    │ - Admin operations    │   │
│  │ - DB auth       │    │ - Column-level  │    │                       │   │
│  └─────────────────┘    └─────────────────┘    └───────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                           All access │ logged
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Audit Log                                       │
│                                                                             │
│  timestamp | principal | action | resource | outcome | details              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Authentication

### Design Options

#### Option 1: Database-Native Authentication

Leverage the underlying database's authentication mechanisms.

**PostgreSQL**:
- Create database users for each Planar user
- Use `pg_hba.conf` for connection policies
- Support LDAP/Kerberos via PostgreSQL extensions

**SQLite**:
- Limited authentication (file permissions only)
- Requires application-level authentication layer

**Advantages**:
- No additional infrastructure
- Well-tested, battle-hardened
- Integrates with existing enterprise auth (LDAP/Kerberos)

**Disadvantages**:
- Database-specific configuration
- Limited for SQLite deployments
- User management tied to database admin

#### Option 2: JWT Token Authentication

Issue JWT tokens that encode identity and permissions.

**Advantages**:
- Stateless authentication
- Works with any database backend
- Easy integration with OAuth/OIDC providers
- Token can carry permissions (reduce authorization lookups)

**Disadvantages**:
- Requires token management infrastructure
- Token revocation is complex
- Additional latency for token validation

#### Option 3: API Key Authentication

Simple API keys for service-to-service communication.

**Advantages**:
- Simple to implement and use
- Good for programmatic access
- Easy to rotate

**Disadvantages**:
- No identity federation
- Key management overhead
- Less secure than certificate-based auth

#### Option 4: mTLS (Mutual TLS)

Certificate-based authentication using X.509 certificates.

**Advantages**:
- Strong cryptographic identity
- Suitable for service mesh environments
- No shared secrets

**Disadvantages**:
- Complex PKI infrastructure
- Certificate management overhead
- Not user-friendly for human users

### Current Recommendation

Support multiple authentication methods:

1. **Primary**: JWT tokens for user authentication (integrates with OIDC providers)
2. **Secondary**: API keys for service-to-service access
3. **Database**: Native database auth for direct DB access
4. **Optional**: mTLS for high-security environments

### Implementation

```rust
/// Authentication principal (who is making the request)
#[derive(Clone, Debug)]
pub struct Principal {
    /// Unique identifier
    pub id: String,
    /// Principal type
    pub principal_type: PrincipalType,
    /// Display name
    pub name: Option<String>,
    /// Email (for users)
    pub email: Option<String>,
    /// Groups/roles this principal belongs to
    pub groups: Vec<String>,
    /// Additional attributes from the identity provider
    pub attributes: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrincipalType {
    /// Human user
    User,
    /// Service account
    Service,
    /// System process (internal)
    System,
    /// Anonymous (unauthenticated)
    Anonymous,
}

/// Authentication provider trait
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Authenticate a request and return the principal
    async fn authenticate(&self, request: &AuthRequest) -> Result<Principal, AuthError>;
    
    /// Validate a token and return claims
    async fn validate_token(&self, token: &str) -> Result<TokenClaims, AuthError>;
}

/// JWT authentication provider
pub struct JwtAuthProvider {
    /// JWKS URI for key validation
    jwks_uri: String,
    /// Issuer claim to validate
    issuer: String,
    /// Audience claim to validate
    audience: String,
    /// Cached JWKS keys
    key_cache: RwLock<JwkCache>,
}

impl JwtAuthProvider {
    pub fn new(jwks_uri: String, issuer: String, audience: String) -> Self {
        Self {
            jwks_uri,
            issuer,
            audience,
            key_cache: RwLock::new(JwkCache::new()),
        }
    }
}

#[async_trait]
impl AuthProvider for JwtAuthProvider {
    async fn authenticate(&self, request: &AuthRequest) -> Result<Principal, AuthError> {
        let token = request.bearer_token()
            .ok_or(AuthError::MissingToken)?;
        
        let claims = self.validate_token(token).await?;
        
        Ok(Principal {
            id: claims.sub.clone(),
            principal_type: PrincipalType::User,
            name: claims.name.clone(),
            email: claims.email.clone(),
            groups: claims.groups.clone().unwrap_or_default(),
            attributes: claims.custom_claims.clone(),
        })
    }
    
    async fn validate_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        // Decode header to get key ID
        let header = jsonwebtoken::decode_header(token)
            .map_err(|e| AuthError::InvalidToken(e.to_string()))?;
        
        let kid = header.kid.ok_or(AuthError::MissingKeyId)?;
        
        // Get key from cache or fetch
        let key = self.get_key(&kid).await?;
        
        // Validate token
        let validation = jsonwebtoken::Validation::new(header.alg);
        let claims = jsonwebtoken::decode::<TokenClaims>(token, &key, &validation)
            .map_err(|e| AuthError::InvalidToken(e.to_string()))?;
        
        // Validate issuer and audience
        if claims.claims.iss != self.issuer {
            return Err(AuthError::InvalidIssuer);
        }
        if !claims.claims.aud.contains(&self.audience) {
            return Err(AuthError::InvalidAudience);
        }
        
        Ok(claims.claims)
    }
}

/// API key authentication provider
pub struct ApiKeyAuthProvider {
    /// Key storage (could be database, cache, etc.)
    key_store: Arc<dyn ApiKeyStore>,
}

#[async_trait]
impl AuthProvider for ApiKeyAuthProvider {
    async fn authenticate(&self, request: &AuthRequest) -> Result<Principal, AuthError> {
        let api_key = request.api_key()
            .ok_or(AuthError::MissingApiKey)?;
        
        // Hash the key for lookup (keys are stored hashed)
        let key_hash = hash_api_key(api_key);
        
        let key_record = self.key_store.get_by_hash(&key_hash).await?
            .ok_or(AuthError::InvalidApiKey)?;
        
        // Check expiration
        if let Some(expires_at) = key_record.expires_at {
            if expires_at < Utc::now() {
                return Err(AuthError::ExpiredApiKey);
            }
        }
        
        // Check if revoked
        if key_record.revoked {
            return Err(AuthError::RevokedApiKey);
        }
        
        Ok(Principal {
            id: key_record.service_account_id,
            principal_type: PrincipalType::Service,
            name: Some(key_record.name),
            email: None,
            groups: key_record.groups,
            attributes: HashMap::new(),
        })
    }
}
```

## Authorization

### Role-Based Access Control (RBAC)

```rust
/// A role that can be assigned to principals
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Role {
    /// Role identifier
    pub name: String,
    /// Human-readable description
    pub description: Option<String>,
    /// Permissions granted by this role
    pub permissions: Vec<Permission>,
}

/// A permission to perform an action on a resource
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Permission {
    /// Action being permitted
    pub action: Action,
    /// Resource the action applies to
    pub resource: ResourcePattern,
}

/// Actions that can be performed
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    // Table operations
    CreateTable,
    DropTable,
    ReadTable,
    WriteTable,
    AlterTable,
    
    // Schema operations
    ReadSchema,
    AlterSchema,
    
    // Data operations
    Select,
    Insert,
    Update,
    Delete,
    
    // Maintenance operations
    Compact,
    Vacuum,
    
    // Admin operations
    ManagePermissions,
    ViewAuditLog,
    
    // Wildcard
    All,
}

/// Resource pattern for matching
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourcePattern {
    /// Resource type
    pub resource_type: ResourceType,
    /// Pattern (supports wildcards)
    pub pattern: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceType {
    Namespace,
    Table,
    Column,
    System,
}

impl ResourcePattern {
    /// Check if a resource matches this pattern
    pub fn matches(&self, resource: &Resource) -> bool {
        if self.resource_type != resource.resource_type {
            return false;
        }
        
        // Support wildcard matching
        if self.pattern == "*" {
            return true;
        }
        
        // Support prefix matching with *
        if self.pattern.ends_with("*") {
            let prefix = &self.pattern[..self.pattern.len() - 1];
            return resource.name.starts_with(prefix);
        }
        
        self.pattern == resource.name
    }
}
```

### Attribute-Based Access Control (ABAC)

For fine-grained policies based on attributes:

```rust
/// ABAC policy for complex authorization rules
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Policy {
    /// Policy identifier
    pub id: String,
    /// Policy name
    pub name: String,
    /// Conditions that must be met
    pub conditions: Vec<Condition>,
    /// Effect when conditions match
    pub effect: Effect,
    /// Actions this policy applies to
    pub actions: Vec<Action>,
    /// Resources this policy applies to
    pub resources: Vec<ResourcePattern>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Condition {
    /// Principal attribute equals value
    PrincipalAttribute { key: String, value: String },
    /// Resource attribute equals value
    ResourceAttribute { key: String, value: String },
    /// Time-based condition
    TimeWindow { start: Option<NaiveTime>, end: Option<NaiveTime> },
    /// IP-based condition
    SourceIp { allowed_cidrs: Vec<String> },
    /// Environment condition
    Environment { key: String, value: String },
    /// Logical AND of conditions
    And(Vec<Condition>),
    /// Logical OR of conditions
    Or(Vec<Condition>),
    /// Logical NOT
    Not(Box<Condition>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    Allow,
    Deny,
}

impl Policy {
    /// Evaluate policy against a request
    pub fn evaluate(&self, request: &AuthzRequest) -> Option<Effect> {
        // Check if policy applies to this action and resource
        if !self.actions.iter().any(|a| a == &request.action || a == &Action::All) {
            return None;
        }
        
        if !self.resources.iter().any(|r| r.matches(&request.resource)) {
            return None;
        }
        
        // Evaluate conditions
        if self.conditions.iter().all(|c| c.evaluate(request)) {
            Some(self.effect.clone())
        } else {
            None
        }
    }
}
```

### Row-Level Security (RLS)

Filter rows based on principal attributes:

```rust
/// Row-level security policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowLevelPolicy {
    /// Policy identifier
    pub id: String,
    /// Table this policy applies to
    pub table_uuid: Uuid,
    /// SQL predicate expression (parameterized)
    pub predicate: String,
    /// Principals this policy applies to (or "*" for all)
    pub applies_to: Vec<String>,
    /// Whether this is a permissive or restrictive policy
    pub policy_type: RlsPolicyType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RlsPolicyType {
    /// Rows matching predicate are allowed (OR with other permissive policies)
    Permissive,
    /// Rows must match predicate (AND with all restrictive policies)
    Restrictive,
}

impl RowLevelPolicy {
    /// Generate the SQL predicate for a principal
    pub fn to_predicate(&self, principal: &Principal) -> String {
        // Replace placeholders with principal attributes
        let mut pred = self.predicate.clone();
        pred = pred.replace("{principal.id}", &principal.id);
        pred = pred.replace("{principal.email}", principal.email.as_deref().unwrap_or(""));
        
        for (key, value) in &principal.attributes {
            pred = pred.replace(&format!("{{principal.{}}}", key), value);
        }
        
        pred
    }
}

/// Example RLS policies:
/// 
/// -- Tenant isolation: users can only see their tenant's data
/// predicate: "tenant_id = '{principal.tenant_id}'"
/// 
/// -- Department-based access
/// predicate: "department IN ({principal.departments})"
/// 
/// -- Data classification
/// predicate: "classification_level <= {principal.clearance_level}"
```

### Column-Level Security

Mask or restrict access to sensitive columns:

```rust
/// Column-level security policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnPolicy {
    /// Policy identifier
    pub id: String,
    /// Table this policy applies to
    pub table_uuid: Uuid,
    /// Column this policy applies to
    pub column_name: String,
    /// Access rule
    pub rule: ColumnAccessRule,
    /// Principals this policy applies to (or "*" for all)
    pub applies_to: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColumnAccessRule {
    /// Column is hidden (not returned in results)
    Hidden,
    /// Column is masked with a function
    Masked(MaskingFunction),
    /// Column is accessible (no restriction)
    Accessible,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MaskingFunction {
    /// Replace with NULL
    Null,
    /// Replace with default value
    Default(String),
    /// Partial masking (e.g., show last 4 digits of SSN)
    Partial { show_first: usize, show_last: usize, mask_char: char },
    /// Hash the value (one-way)
    Hash,
    /// Tokenize (reversible for authorized users)
    Tokenize,
    /// Custom SQL expression
    Custom(String),
}

impl MaskingFunction {
    /// Apply masking to a value
    pub fn apply(&self, value: &ScalarValue, column_type: &DataType) -> ScalarValue {
        match self {
            MaskingFunction::Null => ScalarValue::Null,
            
            MaskingFunction::Default(default_value) => {
                // Parse default value based on column type
                parse_scalar(default_value, column_type).unwrap_or(ScalarValue::Null)
            }
            
            MaskingFunction::Partial { show_first, show_last, mask_char } => {
                if let ScalarValue::String(s) = value {
                    let len = s.len();
                    if len <= show_first + show_last {
                        ScalarValue::String(mask_char.to_string().repeat(len))
                    } else {
                        let first: String = s.chars().take(*show_first).collect();
                        let last: String = s.chars().skip(len - show_last).collect();
                        let middle = mask_char.to_string().repeat(len - show_first - show_last);
                        ScalarValue::String(format!("{}{}{}", first, middle, last))
                    }
                } else {
                    ScalarValue::Null
                }
            }
            
            MaskingFunction::Hash => {
                let bytes = scalar_to_bytes(value).unwrap_or_default();
                let hash = sha256::digest(&bytes);
                ScalarValue::String(hash)
            }
            
            _ => ScalarValue::Null,
        }
    }
}
```

### Authorization Service

```rust
/// Authorization service
pub struct Authorizer {
    /// Role storage
    role_store: Arc<dyn RoleStore>,
    /// Policy storage
    policy_store: Arc<dyn PolicyStore>,
    /// Role assignments
    role_assignments: Arc<dyn RoleAssignmentStore>,
    /// Row-level policies
    rls_policies: Arc<dyn RlsPolicyStore>,
    /// Column-level policies
    column_policies: Arc<dyn ColumnPolicyStore>,
}

impl Authorizer {
    /// Check if a principal is authorized for an action on a resource
    pub async fn authorize(
        &self,
        principal: &Principal,
        action: &Action,
        resource: &Resource,
    ) -> Result<AuthzDecision, AuthzError> {
        // Get principal's roles
        let roles = self.get_roles(principal).await?;
        
        // Check RBAC permissions
        for role in &roles {
            for permission in &role.permissions {
                if permission.action == *action || permission.action == Action::All {
                    if permission.resource.matches(resource) {
                        // Found a matching permission, now check ABAC policies
                        let request = AuthzRequest {
                            principal: principal.clone(),
                            action: action.clone(),
                            resource: resource.clone(),
                            context: HashMap::new(),
                        };
                        
                        // Evaluate ABAC policies
                        let abac_decision = self.evaluate_policies(&request).await?;
                        
                        match abac_decision {
                            Some(Effect::Deny) => return Ok(AuthzDecision::Denied("Policy denied".into())),
                            Some(Effect::Allow) | None => return Ok(AuthzDecision::Allowed),
                        }
                    }
                }
            }
        }
        
        // No matching permission found
        Ok(AuthzDecision::Denied("No permission".into()))
    }
    
    /// Get row-level security predicates for a table read
    pub async fn get_rls_predicates(
        &self,
        principal: &Principal,
        table_uuid: Uuid,
    ) -> Result<Vec<String>, AuthzError> {
        let policies = self.rls_policies.get_for_table(table_uuid).await?;
        
        let mut permissive_preds = Vec::new();
        let mut restrictive_preds = Vec::new();
        
        for policy in policies {
            if policy.applies_to_principal(principal) {
                let pred = policy.to_predicate(principal);
                match policy.policy_type {
                    RlsPolicyType::Permissive => permissive_preds.push(pred),
                    RlsPolicyType::Restrictive => restrictive_preds.push(pred),
                }
            }
        }
        
        // Combine: (permissive1 OR permissive2 OR ...) AND restrictive1 AND restrictive2 AND ...
        let mut result = Vec::new();
        
        if !permissive_preds.is_empty() {
            result.push(format!("({})", permissive_preds.join(" OR ")));
        }
        
        result.extend(restrictive_preds);
        
        Ok(result)
    }
    
    /// Get column masking rules for a principal
    pub async fn get_column_masks(
        &self,
        principal: &Principal,
        table_uuid: Uuid,
    ) -> Result<HashMap<String, MaskingFunction>, AuthzError> {
        let policies = self.column_policies.get_for_table(table_uuid).await?;
        
        let mut masks = HashMap::new();
        
        for policy in policies {
            if policy.applies_to_principal(principal) {
                match &policy.rule {
                    ColumnAccessRule::Masked(func) => {
                        masks.insert(policy.column_name.clone(), func.clone());
                    }
                    ColumnAccessRule::Hidden => {
                        // Hidden columns are excluded from projection
                    }
                    ColumnAccessRule::Accessible => {
                        // No masking
                    }
                }
            }
        }
        
        Ok(masks)
    }
}
```

## Schema Changes

### Authorization Tables

```sql
-- Roles
CREATE TABLE IF NOT EXISTS security_roles (
    role_name TEXT PRIMARY KEY,
    description TEXT,
    permissions TEXT NOT NULL, -- JSON array of Permission
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Role assignments (principal -> role)
CREATE TABLE IF NOT EXISTS security_role_assignments (
    principal_id TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    role_name TEXT NOT NULL,
    assigned_at TIMESTAMP NOT NULL,
    assigned_by TEXT,
    PRIMARY KEY (principal_id, role_name),
    FOREIGN KEY (role_name) REFERENCES security_roles(role_name)
);

CREATE INDEX IF NOT EXISTS idx_role_assignments_principal 
    ON security_role_assignments(principal_id);

-- ABAC Policies
CREATE TABLE IF NOT EXISTS security_policies (
    policy_id TEXT PRIMARY KEY,
    policy_name TEXT NOT NULL,
    conditions TEXT NOT NULL, -- JSON array of Condition
    effect TEXT NOT NULL, -- 'allow' or 'deny'
    actions TEXT NOT NULL, -- JSON array of Action
    resources TEXT NOT NULL, -- JSON array of ResourcePattern
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Row-level security policies
CREATE TABLE IF NOT EXISTS security_rls_policies (
    policy_id TEXT PRIMARY KEY,
    table_uuid BLOB NOT NULL,
    predicate TEXT NOT NULL,
    applies_to TEXT NOT NULL, -- JSON array of principal patterns
    policy_type TEXT NOT NULL, -- 'permissive' or 'restrictive'
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    FOREIGN KEY (table_uuid) REFERENCES tables(table_uuid)
);

CREATE INDEX IF NOT EXISTS idx_rls_policies_table
    ON security_rls_policies(table_uuid);

-- Column-level security policies
CREATE TABLE IF NOT EXISTS security_column_policies (
    policy_id TEXT PRIMARY KEY,
    table_uuid BLOB NOT NULL,
    column_name TEXT NOT NULL,
    rule_type TEXT NOT NULL, -- 'hidden', 'masked', 'accessible'
    masking_config TEXT, -- JSON MaskingFunction config
    applies_to TEXT NOT NULL, -- JSON array of principal patterns
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    FOREIGN KEY (table_uuid) REFERENCES tables(table_uuid)
);

CREATE INDEX IF NOT EXISTS idx_column_policies_table
    ON security_column_policies(table_uuid);

-- API Keys
CREATE TABLE IF NOT EXISTS security_api_keys (
    key_id TEXT PRIMARY KEY,
    key_hash TEXT NOT NULL UNIQUE, -- SHA-256 hash of the key
    service_account_id TEXT NOT NULL,
    name TEXT NOT NULL,
    groups TEXT NOT NULL, -- JSON array of group names
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at TIMESTAMP,
    last_used_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash
    ON security_api_keys(key_hash);

CREATE INDEX IF NOT EXISTS idx_api_keys_service
    ON security_api_keys(service_account_id);
```

## Encryption

### Encryption at Rest

```rust
/// Encryption configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Enable encryption at rest
    pub enabled: bool,
    /// Key management type
    pub key_management: KeyManagement,
    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KeyManagement {
    /// AWS KMS
    AwsKms { key_arn: String, region: String },
    /// Google Cloud KMS
    GcpKms { key_name: String },
    /// Azure Key Vault
    AzureKeyVault { vault_url: String, key_name: String },
    /// HashiCorp Vault
    HashicorpVault { address: String, path: String },
    /// Local key (for development only)
    Local { key_file: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    Aes256Gcm,
    Aes256Cbc,
    ChaCha20Poly1305,
}
```

### Data Encryption Key (DEK) Management

```rust
/// Envelope encryption: DEK encrypted by KEK (Key Encryption Key)
pub struct EnvelopeEncryption {
    kms_client: Arc<dyn KmsClient>,
    dek_cache: RwLock<HashMap<Uuid, DecryptedDek>>,
}

impl EnvelopeEncryption {
    /// Generate a new DEK for a table
    pub async fn generate_dek(&self, table_uuid: Uuid) -> Result<EncryptedDek, EncryptionError> {
        // Generate random DEK
        let mut dek = vec![0u8; 32]; // 256 bits
        rand::thread_rng().fill_bytes(&mut dek);
        
        // Encrypt DEK with KMS
        let encrypted_dek = self.kms_client.encrypt(&dek).await?;
        
        // Store encrypted DEK
        Ok(EncryptedDek {
            table_uuid,
            encrypted_key: encrypted_dek,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            created_at: Utc::now(),
        })
    }
    
    /// Get DEK for encrypting/decrypting table data
    pub async fn get_dek(&self, table_uuid: Uuid) -> Result<Vec<u8>, EncryptionError> {
        // Check cache
        {
            let cache = self.dek_cache.read().await;
            if let Some(dek) = cache.get(&table_uuid) {
                if dek.expires_at > Utc::now() {
                    return Ok(dek.key.clone());
                }
            }
        }
        
        // Fetch encrypted DEK from storage
        let encrypted_dek = self.get_encrypted_dek(table_uuid).await?;
        
        // Decrypt with KMS
        let dek = self.kms_client.decrypt(&encrypted_dek.encrypted_key).await?;
        
        // Cache for future use
        {
            let mut cache = self.dek_cache.write().await;
            cache.insert(table_uuid, DecryptedDek {
                key: dek.clone(),
                expires_at: Utc::now() + Duration::hours(1),
            });
        }
        
        Ok(dek)
    }
    
    /// Encrypt data with table's DEK
    pub async fn encrypt_data(
        &self,
        table_uuid: Uuid,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        let dek = self.get_dek(table_uuid).await?;
        
        // Generate random nonce
        let mut nonce = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce);
        
        // Encrypt with AES-256-GCM
        let cipher = Aes256Gcm::new_from_slice(&dek)?;
        let ciphertext = cipher.encrypt(&nonce.into(), plaintext)?;
        
        // Return nonce + ciphertext
        let mut result = Vec::with_capacity(12 + ciphertext.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&ciphertext);
        
        Ok(result)
    }
    
    /// Decrypt data with table's DEK
    pub async fn decrypt_data(
        &self,
        table_uuid: Uuid,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        if ciphertext.len() < 12 {
            return Err(EncryptionError::InvalidCiphertext);
        }
        
        let dek = self.get_dek(table_uuid).await?;
        
        let nonce = &ciphertext[..12];
        let encrypted = &ciphertext[12..];
        
        let cipher = Aes256Gcm::new_from_slice(&dek)?;
        let plaintext = cipher.decrypt(nonce.into(), encrypted)?;
        
        Ok(plaintext)
    }
}
```

### Encryption in Transit

```rust
/// TLS configuration for connections
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// Certificate file path
    pub cert_file: String,
    /// Key file path
    pub key_file: String,
    /// CA certificate file (for client verification)
    pub ca_file: Option<String>,
    /// Require client certificates (mTLS)
    pub require_client_cert: bool,
    /// Minimum TLS version
    pub min_version: TlsVersion,
}

#[derive(Clone, Debug)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}
```

## Audit Logging

### Audit Log Schema

```sql
CREATE TABLE IF NOT EXISTS audit_log (
    event_id TEXT PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    principal_id TEXT,
    principal_type TEXT,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    outcome TEXT NOT NULL, -- 'success', 'denied', 'error'
    client_ip TEXT,
    user_agent TEXT,
    request_id TEXT,
    duration_ms INTEGER,
    details TEXT, -- JSON with additional context
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp
    ON audit_log(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_audit_log_principal
    ON audit_log(principal_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_audit_log_resource
    ON audit_log(resource_type, resource_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_audit_log_action
    ON audit_log(action, event_timestamp);
```

### Audit Logger

```rust
/// Audit event
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    pub event_timestamp: DateTime<Utc>,
    pub principal_id: Option<String>,
    pub principal_type: Option<String>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub outcome: AuditOutcome,
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
    pub request_id: Option<String>,
    pub duration_ms: Option<u64>,
    pub details: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AuditOutcome {
    Success,
    Denied,
    Error,
}

/// Audit logger
pub struct AuditLogger {
    /// Audit storage backend
    storage: Arc<dyn AuditStorage>,
    /// Async event buffer
    buffer: mpsc::Sender<AuditEvent>,
}

impl AuditLogger {
    /// Log an audit event
    pub fn log(&self, event: AuditEvent) {
        // Non-blocking send to buffer
        let _ = self.buffer.try_send(event);
    }
    
    /// Log a data access event
    pub fn log_data_access(
        &self,
        principal: &Principal,
        table: &Table,
        action: Action,
        outcome: AuditOutcome,
        request_ctx: &RequestContext,
    ) {
        self.log(AuditEvent {
            event_id: Uuid::new_v4().to_string(),
            event_timestamp: Utc::now(),
            principal_id: Some(principal.id.clone()),
            principal_type: Some(format!("{:?}", principal.principal_type)),
            action: format!("{:?}", action),
            resource_type: "table".to_string(),
            resource_id: Some(table.table_uuid.to_string()),
            outcome,
            client_ip: request_ctx.client_ip.clone(),
            user_agent: request_ctx.user_agent.clone(),
            request_id: request_ctx.request_id.clone(),
            duration_ms: None,
            details: Some(serde_json::json!({
                "table_name": table.table_name,
                "namespace": table.namespace,
            })),
        });
    }
    
    /// Log a permission change event
    pub fn log_permission_change(
        &self,
        principal: &Principal,
        change: &PermissionChange,
        outcome: AuditOutcome,
        request_ctx: &RequestContext,
    ) {
        self.log(AuditEvent {
            event_id: Uuid::new_v4().to_string(),
            event_timestamp: Utc::now(),
            principal_id: Some(principal.id.clone()),
            principal_type: Some(format!("{:?}", principal.principal_type)),
            action: "ManagePermissions".to_string(),
            resource_type: change.resource_type.clone(),
            resource_id: change.resource_id.clone(),
            outcome,
            client_ip: request_ctx.client_ip.clone(),
            user_agent: request_ctx.user_agent.clone(),
            request_id: request_ctx.request_id.clone(),
            duration_ms: None,
            details: Some(serde_json::to_value(change).unwrap()),
        });
    }
}

/// Background audit log writer
async fn audit_log_writer(
    mut receiver: mpsc::Receiver<AuditEvent>,
    storage: Arc<dyn AuditStorage>,
) {
    let mut buffer = Vec::with_capacity(100);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    
    loop {
        tokio::select! {
            Some(event) = receiver.recv() => {
                buffer.push(event);
                if buffer.len() >= 100 {
                    let events = std::mem::take(&mut buffer);
                    let _ = storage.write_batch(events).await;
                }
            }
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    let events = std::mem::take(&mut buffer);
                    let _ = storage.write_batch(events).await;
                }
            }
        }
    }
}
```

## External Access Security

For direct database access ([external_access.md](external_access.md)):

```rust
/// Security layer for external database access
pub struct ExternalAccessSecurity {
    /// Row-level security view generator
    rls_views: RlsViewGenerator,
    /// Column masking view generator
    masking_views: MaskingViewGenerator,
}

impl ExternalAccessSecurity {
    /// Create secure views for external access
    pub async fn create_secure_views(
        &self,
        principal: &Principal,
        table: &Table,
    ) -> Result<SecureView, SecurityError> {
        // Get RLS predicates
        let rls_predicates = self.get_rls_predicates(principal, table.table_uuid).await?;
        
        // Get column masks
        let column_masks = self.get_column_masks(principal, table.table_uuid).await?;
        
        // Generate view SQL
        let view_sql = self.generate_view_sql(table, &rls_predicates, &column_masks)?;
        
        Ok(SecureView {
            view_name: format!("secure_{}_{}", principal.id, table.table_name),
            view_sql,
            expires_at: Utc::now() + Duration::hours(1),
        })
    }
    
    fn generate_view_sql(
        &self,
        table: &Table,
        rls_predicates: &[String],
        column_masks: &HashMap<String, MaskingFunction>,
    ) -> Result<String, SecurityError> {
        let schema = self.get_table_schema(table.table_uuid)?;
        
        // Build column list with masking
        let columns: Vec<String> = schema.columns.iter()
            .filter_map(|col| {
                if let Some(mask) = column_masks.get(&col.column_name) {
                    Some(self.mask_column_sql(&col.column_name, mask))
                } else {
                    Some(col.column_name.clone())
                }
            })
            .collect();
        
        // Build WHERE clause
        let where_clause = if rls_predicates.is_empty() {
            "TRUE".to_string()
        } else {
            rls_predicates.join(" AND ")
        };
        
        Ok(format!(
            "SELECT {} FROM {} WHERE {}",
            columns.join(", "),
            table.table_name,
            where_clause
        ))
    }
}
```

## Implementation Phases

### Phase 1: Basic Authentication

1. Implement JWT token validation
2. Implement API key authentication
3. Add principal extraction to request handling
4. Basic audit logging for all requests

### Phase 2: Role-Based Access Control

1. Create security tables schema
2. Implement role storage and management
3. Implement role assignment
4. Add authorization checks to catalog operations

### Phase 3: Row and Column Security

1. Implement row-level security policies
2. Implement column masking
3. Integrate RLS with query execution
4. Add policy management API

### Phase 4: Encryption

1. Implement envelope encryption
2. Integrate with KMS providers (AWS, GCP, Azure)
3. Add encryption to file writers
4. Add decryption to file readers

### Phase 5: Enterprise Features

1. ABAC policy engine
2. External authorization (OPA integration)
3. Advanced audit analytics
4. Compliance reporting

## Testing Strategy

### Unit Tests

- JWT token validation (valid, expired, invalid signature)
- Authorization decision logic
- RLS predicate generation
- Column masking functions

### Integration Tests

- End-to-end authentication flows
- Authorization with role assignments
- RLS filtering correctness
- Encryption round-trip

### Security Tests

- Penetration testing
- Token fuzzing
- SQL injection in RLS predicates
- Privilege escalation attempts

## Open Questions

1. **Session management**: How do we handle long-running sessions? Token refresh? Session tokens?

2. **Federation**: How do we support identity federation across multiple organizations?

3. **Key rotation**: How do we rotate encryption keys without downtime?

4. **Cross-table RLS**: Can RLS policies reference other tables for dynamic access control?

5. **Performance impact**: What's the latency impact of RLS/column masking? Should we cache authorization decisions?

## References

- [PostgreSQL Row Level Security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- [Apache Ranger](https://ranger.apache.org/)
- [Open Policy Agent](https://www.openpolicyagent.org/)
- [AWS KMS](https://docs.aws.amazon.com/kms/)
- [OAuth 2.0 / OIDC](https://oauth.net/2/)
- [external_access.md](external_access.md) - External engine security
- [db_control_plane.md](db_control_plane.md) - Control plane architecture
