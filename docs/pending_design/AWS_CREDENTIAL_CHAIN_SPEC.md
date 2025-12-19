# AWS Credential Chain for S3 Access

**Date:** 2025-12-17
**Status:** Research Complete, Implementation Pending
**Priority:** Medium (enables cloud deployment)

## Summary

Configure Thunderduck to access S3 using IAM credentials inherited from EC2 instance profiles, EKS pod IAM roles (IRSA), or environment variables via DuckDB's credential chain provider.

## Current State

Thunderduck has **no AWS/S3 configuration** currently. S3 support is documented as a future enhancement in `docs/dev_journal/M24_WRITE_OPERATION.md`. All file operations are local-path only.

---

## Requirements

### Deployment Scenarios

| Environment | Credential Source | DuckDB Chain |
|-------------|-------------------|--------------|
| EC2 | Instance profile (IMDS) | `instance` |
| EKS | Web identity token (IRSA) | `sts` |
| Local dev | Environment variables | `env` |
| Local dev | AWS config files | `config` |
| AWS SSO | SSO credentials | `sso` |

### What Needs to Be True

1. **Extensions loaded**: `aws` and `httpfs` extensions must be installed and loaded
2. **Secret created**: S3 secret with `credential_chain` provider
3. **IAM permissions**: Role must have S3 read/write permissions for target buckets
4. **Network access**: IMDS (169.254.169.254) accessible for instance profiles

---

## DuckDB Configuration

### Load Required Extensions

```sql
INSTALL aws;
LOAD aws;
INSTALL httpfs;
LOAD httpfs;
```

### Create S3 Secret

**Universal (recommended):**
```sql
CREATE OR REPLACE SECRET thunderduck_s3 (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env;config;sts;instance'
);
```

**EC2 Instance Profile only:**
```sql
CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'instance'
);
```

**EKS Pod with IRSA only:**
```sql
CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'sts'
);
```

### Chain Options

| Chain | Description | Use Case |
|-------|-------------|----------|
| `env` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Local dev, CI/CD |
| `config` | `~/.aws/credentials`, `~/.aws/config` | Local dev with profiles |
| `sts` | Web identity token file | EKS with IRSA |
| `instance` | EC2 metadata service | EC2, ECS |
| `sso` | AWS SSO credentials | Enterprise SSO |
| `process` | External credential process | Custom providers |

### Additional Options

```sql
CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env;sts;instance',
    REGION 'us-west-2',           -- Override auto-detected region
    ENDPOINT 's3.us-west-2.amazonaws.com',  -- Custom endpoint
    URL_STYLE 'path'              -- For S3-compatible storage
);
```

---

## Implementation Plan

### 1. Configuration Properties

Add to `connect-server.properties`:

```properties
# AWS S3 Configuration
thunderduck.aws.enabled=false
thunderduck.aws.credential-chain=env;config;sts;instance
thunderduck.aws.region=
thunderduck.aws.endpoint=
```

### 2. DuckDBRuntime.java Changes

```java
private void configureAWSAccess() throws SQLException {
    String awsEnabled = System.getProperty("thunderduck.aws.enabled", "false");
    if (!"true".equalsIgnoreCase(awsEnabled)) {
        logger.debug("AWS S3 access disabled");
        return;
    }

    try (Statement stmt = connection.createStatement()) {
        // Install and load extensions
        stmt.execute("INSTALL aws");
        stmt.execute("LOAD aws");
        stmt.execute("INSTALL httpfs");
        stmt.execute("LOAD httpfs");

        // Build credential chain
        String chain = System.getProperty(
            "thunderduck.aws.credential-chain",
            "env;config;sts;instance"
        );

        StringBuilder secretSQL = new StringBuilder();
        secretSQL.append("CREATE OR REPLACE SECRET thunderduck_s3 (");
        secretSQL.append("TYPE s3, ");
        secretSQL.append("PROVIDER credential_chain, ");
        secretSQL.append("CHAIN '").append(chain).append("'");

        // Optional region override
        String region = System.getProperty("thunderduck.aws.region");
        if (region != null && !region.isEmpty()) {
            secretSQL.append(", REGION '").append(region).append("'");
        }

        secretSQL.append(")");

        stmt.execute(secretSQL.toString());
        logger.info("AWS S3 access configured with chain: {}", chain);

    } catch (SQLException e) {
        logger.warn("Failed to configure AWS access: {}. S3 operations will fail.",
            e.getMessage());
        // Don't throw - allow server to start without S3 support
    }
}
```

### 3. Call from configureConnection()

```java
private void configureConnection() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
        // ... existing configuration ...

        // Configure AWS access (optional, fails gracefully)
        configureAWSAccess();
    }
}
```

---

## Known Issues (2025)

### EKS IRSA 403 Errors

**Issue:** [duckdb-aws#93](https://github.com/duckdb/duckdb-aws/issues/93) (July 2025)

S3 access with `credential_chain` provider returns HTTP 403 on EKS pods with IAM roles attached.

**Status:** Open

**Workaround:** Manual credential injection via boto3:
```python
import boto3
session = boto3.Session()
credentials = session.get_credentials()
# Pass to DuckDB via config provider
```

### Web Identity Token Support

**Issue:** [duckdb-aws#31](https://github.com/duckdb/duckdb-aws/issues/31)

`AWS_WEB_IDENTITY_TOKEN_FILE` not automatically used by credential chain.

**Status:** Partial support via `CHAIN 'sts'`

**Workaround:** Explicitly specify `sts` in chain order.

---

## Testing Plan

### Unit Tests

1. Extension loading succeeds
2. Secret creation with various chain configurations
3. Graceful failure when extensions unavailable

### Integration Tests

| Test | Environment | Expected |
|------|-------------|----------|
| Read from S3 | Local with env vars | Success |
| Write to S3 | Local with env vars | Success |
| Read from S3 | EC2 instance | Success |
| Read from S3 | EKS pod | Success (verify #93 workaround) |
| Missing credentials | Any | Graceful error message |

### Manual Verification

```sql
-- Verify extensions loaded
SELECT * FROM duckdb_extensions() WHERE loaded = true;

-- Verify secret created
SELECT * FROM duckdb_secrets();

-- Test S3 access
SELECT * FROM read_parquet('s3://my-bucket/test.parquet') LIMIT 1;
```

---

## Security Considerations

1. **Principle of least privilege**: IAM roles should only grant access to required buckets
2. **No credential logging**: Never log AWS credentials or secret contents
3. **Credential rotation**: Instance profile credentials rotate automatically
4. **Network security**: IMDS v2 recommended for EC2 (IMDSv1 vulnerable to SSRF)

---

## Files to Modify

| File | Changes |
|------|---------|
| `core/src/main/java/com/thunderduck/runtime/DuckDBRuntime.java` | Add `configureAWSAccess()` method |
| `connect-server/src/main/resources/connect-server.properties` | Add AWS config properties |
| `docs/dev_journal/M24_WRITE_OPERATION.md` | Update S3 status |

---

## References

- [DuckDB AWS Extension](https://duckdb.org/docs/stable/core_extensions/aws)
- [DuckDB S3 API](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api)
- [AWS Default Credential Provider Chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)
- [EKS IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
- [GitHub Issue: credential_chain on EKS](https://github.com/duckdb/duckdb-aws/issues/93)
- [GitHub Issue: Web identity token support](https://github.com/duckdb/duckdb-aws/issues/31)
