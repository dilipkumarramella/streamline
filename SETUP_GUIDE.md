# Streamline — Setup Guide

> Complete setup from scratch. Estimated time: 2–3 hours.

> **Note on Kafka credentials:** In this project, Kafka credentials are stored in `config.py` for simplicity during development on a free trial. In production, these should be stored in Azure Key Vault backed by a Databricks secret scope and read via `dbutils.secrets.get()`.

---

## Prerequisites

- Azure account (free trial or paid)
- Confluent Cloud account (free trial — $400 credits)
- GitHub account
- PowerBI Desktop (for dashboards)
- Databricks CLI installed locally

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks --version
```

---

## Table of Contents

1. [Azure Resources](#1-azure-resources)
2. [Databricks Setup](#2-databricks-setup)
3. [Unity Catalog Setup](#3-unity-catalog-setup)
4. [Confluent Kafka Setup](#4-confluent-kafka-setup)
5. [Repository Setup](#5-repository-setup)
6. [Config & Credentials](#6-config--credentials)
7. [DAB Deployment](#7-dab-deployment)
8. [CI/CD Setup](#8-cicd-setup)
9. [Run the Platform](#9-run-the-platform)
10. [PowerBI Connection](#10-powerbi-connection)

---

## 1. Azure Resources

### 1.1 Resource Group

```
Azure Portal → Resource Groups → Create
Name:     streamline-dev-rg
Region:   South India (or your preferred region)
```

### 1.2 ADLS Gen2 Storage Account

```
Azure Portal → Storage Accounts → Create
Resource group:  streamline-dev-rg
Name:            streamlinedevstorage  (must be globally unique)
Region:          South India
Performance:     Standard
Redundancy:      LRS (sufficient for dev)
Hierarchical namespace: ENABLED  ← critical for ADLS Gen2
```

After creation, create the container and folders:

```
Storage Account → Containers → + Container
Name: streamline
Access level: Private

Inside streamline container, create these virtual folders:
  bronze/
  silver/
  gold/
  checkpoints/
```

### 1.3 Access Connector for Databricks

This allows Databricks to access ADLS using a managed identity — no storage keys needed.

```
Azure Portal → Create Resource → Search "Access Connector for Azure Databricks"
Resource group: streamline-dev-rg
Name:           streamline-dev-connector
Region:         South India
```

After creation, assign the role:

```
Storage Account (streamlinedevstorage)
→ Access Control (IAM)
→ Add role assignment
Role:    Storage Blob Data Contributor
Assign to: Managed Identity
Select:  streamline-dev-connector
```

### 1.4 Databricks Workspace (Premium)

Unity Catalog requires Databricks Premium.

```
Azure Portal → Create Resource → Azure Databricks
Resource group:  streamline-dev-rg
Workspace name:  streamline-dev-dws
Region:          South India
Pricing tier:    Premium  ← required for Unity Catalog
```

> **Note:** Premium creates a NAT Gateway automatically — this costs ~₹100–150/day on Azure free trial. Monitor your credits daily.

---

## 2. Databricks Setup

### 2.1 Link Access Connector to Databricks

```
Databricks Workspace → Catalog → External Locations → Credentials
→ Create Credential
Type:                Azure Managed Identity
Credential name:     streamline-adls-credential
Access connector ID: /subscriptions/<sub-id>/resourceGroups/streamline-dev-rg/
                     providers/Microsoft.Databricks/accessConnectors/streamline-dev-connector
```

### 2.2 Create External Location

```
Databricks Workspace → Catalog → External Locations
→ Create External Location
Name:        streamline-adls
URL:         abfss://streamline@streamlinedevstorage.dfs.core.windows.net/
Credential:  streamline-adls-credential
```

Test the connection — should show green.

### 2.3 Create Cluster

```
Databricks Workspace → Compute → Create Cluster
Name:           streamline-dev-cluster
Policy:         Unrestricted
Mode:           Single Node
Node type:      Standard_D4ds_v5
Runtime:        17.3 LTS (Scala 2.12, Spark 3.5)
Photon:         OFF
Auto-terminate: 30 minutes

Advanced Options → Spark Config:
spark.databricks.dataLineage.enabled true

Advanced Options → Maven Libraries:
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

### 2.4 Create Service Principal (for job run_as)

```
Azure Portal → Azure Active Directory → App Registrations → New Registration
Name: streamline-sp
```

After creation:
```
→ Certificates & Secrets → New Client Secret
→ Copy the secret value immediately (shown once only)
→ Note the Application (client) ID and Tenant ID
```

Add SP to Databricks:
```
Databricks Workspace → Settings → Identity & Access → Service Principals
→ Add Service Principal
→ Enter Application ID from above
→ Assign role: Contributor
```

---

## 3. Unity Catalog Setup

### 3.1 Create Metastore (if not exists)

```
Databricks Account Console (accounts.azuredatabricks.net)
→ Data → Create Metastore
Name:    streamline-metastore
Region:  South India
ADLS path: abfss://streamline@streamlinedevstorage.dfs.core.windows.net/metastore/
```

Attach to workspace:
```
→ Assign to Workspace → streamline-dev-dws
```

### 3.2 Create Catalog

```sql
-- Run in Databricks notebook or SQL editor
CREATE CATALOG IF NOT EXISTS streamline;
```

### 3.3 Create Schemas

```sql
CREATE SCHEMA IF NOT EXISTS streamline.bronze;
CREATE SCHEMA IF NOT EXISTS streamline.silver;
CREATE SCHEMA IF NOT EXISTS streamline.gold;
```

### 3.4 Grant Permissions

```sql
-- Grant to your user
GRANT ALL PRIVILEGES ON CATALOG streamline TO `your-email@domain.com`;

-- Grant to service principal
GRANT ALL PRIVILEGES ON CATALOG streamline TO `<sp-application-id>`;
```

---

## 4. Confluent Kafka Setup

### 4.1 Create Cluster

```
Confluent Cloud (confluent.io) → Environments → Add Cloud Environment
Name: streamline-env

→ Create Cluster
Type:     Basic
Provider: Azure
Region:   East US (or closest to your Databricks region)
Name:     streamline-kafka
```

### 4.2 Create Topics

```
Cluster → Topics → Add Topic

Topic 1:
  Name:       orders
  Partitions: 3
  Retention:  7 days

Topic 2:
  Name:       payments
  Partitions: 3
  Retention:  7 days

Topic 3:
  Name:       clickstream
  Partitions: 3
  Retention:  7 days
```

### 4.3 Create API Key

```
Cluster → API Keys → Create Key
Scope: Global access
→ Save the API Key and API Secret immediately
```

### 4.4 Get Bootstrap Server

```
Cluster → Cluster Settings → Identification
→ Copy Bootstrap server URL
Format: pkc-xxxxx.eastus.azure.confluent.cloud:9092
```

---

## 5. Repository Setup

### 5.1 Clone the Repository

```bash
git clone https://github.com/dilipkumarramella/streamline.git
cd streamline
```

### 5.2 Connect to Databricks CLI

```bash
databricks configure --host https://<your-workspace-url>
# Enter your personal access token when prompted

# Generate token:
# Databricks Workspace → Settings → User Settings → Access Tokens → Generate
```

### 5.3 Set Up Git in Databricks

```
Databricks Workspace → Settings → Linked Accounts → Git
→ Link GitHub account
→ Authorize Databricks
```

### 5.4 Clone Repo in Databricks Workspace

```
Databricks Workspace → Workspace → Repos → Add Repo
URL: https://github.com/dilipkumarramella/streamline.git
Branch: develop
```

---

## 6. Config & Credentials

### 6.1 Update config.py

Open `resources/notebooks/utils/config.py` and fill in your values for both `dev` and `prod`:

```python
# Storage
"storage_account": "streamlinedevstorage",  # your storage account name
"bronze_path": "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/bronze/",
"silver_path": "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/silver/",
"gold_path":   "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/gold/",
"checkpoint_path": "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/checkpoints/",

# Kafka
"kafka_bootstrap_servers": "pkc-xxxxx.eastus.azure.confluent.cloud:9092",
"kafka_api_key":           "your-api-key",
"kafka_api_secret":        "your-api-secret",
```

> **Production best practice:** Store `kafka_api_key` and `kafka_api_secret` in Azure Key Vault, create a Databricks secret scope linked to it, and read via `dbutils.secrets.get(scope="streamline-secrets", key="kafka-api-key")`. Config file approach used here for free trial simplicity.

### 6.2 Update databricks.yml

```yaml
targets:
  dev:
    workspace:
      host: https://<your-dev-workspace-url>
      state_path: /Workspace/Users/<your-username>/.bundle/streamline/dev

  prod:
    workspace:
      host: https://<your-prod-workspace-url>
      state_path: /Workspace/Shared/streamline/.bundle/prod
```

### 6.3 Update variables.yml

```yaml
variables:
  run_as_user:
    default: "your-email@domain.com"  # or service principal app ID
  service_principal_id:
    default: "your-sp-application-id"
```

### 6.4 Update dev.yml

```yaml
targets:
  dev:
    permissions:
      - user_name: your-email@domain.com
        level: CAN_MANAGE
```

---

## 7. DAB Deployment

### 7.1 Validate Bundle

```bash
# Validate dev
databricks bundle validate --target dev

# Validate prod
databricks bundle validate --target prod
```

Fix any errors before deploying.

### 7.2 Deploy to Dev

```bash
databricks bundle deploy --target dev
```

This creates all jobs, DLT pipeline, and schemas in your dev workspace.

### 7.3 Verify Deployment

```
Databricks Workspace → Workflows → Jobs
Should see:
  - data_generator_and_bronze_continuous
  - streamline_batch_orchestrator_job
  - streamline_clickstream_pipeline
  - streamline_payments_pipeline
  - streamline_customers_pipeline
  - streamline_orders_silver
  - streamline_products_pipeline
  - streamline_vacuum_job

Databricks Workspace → Delta Live Tables
Should see:
  - streamline_dlt_pipeline
```

---

## 8. CI/CD Setup

### 8.1 GitHub Secrets

```
GitHub → Repository → Settings → Secrets and Variables → Actions
→ New Repository Secret

DATABRICKS_HOST          = https://<dev-workspace-url>
DATABRICKS_TOKEN         = <dev-personal-access-token>
DATABRICKS_HOST_PROD     = https://<prod-workspace-url>
DATABRICKS_TOKEN_PROD    = <prod-personal-access-token>
```

### 8.2 Branch Protection

```
GitHub → Repository → Settings → Branches
→ Add Branch Protection Rule

Branch name: develop
✓ Require pull request before merging
✓ Require status checks to pass (select: validate_and_test)
✓ Restrict direct pushes

Repeat for: main
```

### 8.3 Verify CI/CD

```bash
# Create a feature branch and open a PR to develop
git checkout -b feature/test-cicd
git commit --allow-empty -m "test: verify CI/CD"
git push origin feature/test-cicd
# Open PR → GitHub Actions should trigger automatically
```

---

## 9. Run the Platform

### 9.1 Start Data Ingestion

```
Databricks Workspace → Workflows → data_generator_and_bronze_continuous
→ Run Now
→ Set widget: sleep_seconds = 30
```

### 9.2 Start DLT Streaming

```
Databricks Workspace → Delta Live Tables → streamline_dlt_pipeline
→ Start
```

### 9.3 Verify Bronze is Receiving Data

```sql
-- Run in Databricks notebook
SELECT COUNT(*), MAX(ingested_at)
FROM streamline.bronze.orders;
```

### 9.4 Run Silver + Gold Batch

Either wait for the 2AM schedule or trigger manually:

```
Databricks Workspace → Workflows → streamline_batch_orchestrator_job
→ Run Now
```

### 9.5 Verify End to End

```sql
-- Bronze
SELECT COUNT(*) FROM streamline.bronze.orders;
SELECT COUNT(*) FROM streamline.bronze.payments;
SELECT COUNT(*) FROM streamline.bronze.clickstream;

-- Silver
SELECT COUNT(*) FROM streamline.silver.fact_orders;
SELECT COUNT(*) FROM streamline.silver.dim_customer;

-- Gold
SELECT COUNT(*) FROM streamline.gold.orders_daily_summary;
SELECT COUNT(*) FROM streamline.gold.customer_360;

-- Pipeline state
SELECT * FROM streamline.silver.pipeline_state
ORDER BY last_run_time DESC LIMIT 10;
```

---

## 10. PowerBI Connection

### 10.1 Get Databricks Connection Details

```
Databricks Workspace → SQL Warehouses (or Cluster)
→ Connection Details
→ Copy: Server hostname, HTTP path
```

### 10.2 Connect PowerBI Desktop

```
PowerBI Desktop → Get Data → Azure Databricks
Server hostname: adb-xxxxx.azuredatabricks.net
HTTP path:       /sql/1.0/warehouses/xxxxx
Authentication:  Personal Access Token
Token:           <your-databricks-pat>
```

### 10.3 Load Gold Tables

```
Navigator → streamline → gold
Select:
  ✓ orders_daily_summary
  ✓ customer_360
  ✓ funnel_metrics
  ✓ payment_success_rate
  ✓ data_quality_metrics
→ Load
```

### 10.4 Schedule Refresh

```
PowerBI Service (app.powerbi.com)
→ Publish report from PowerBI Desktop
→ Dataset Settings → Scheduled Refresh
→ Set daily refresh after 3AM (after Gold batch completes)
```

---

## Troubleshooting Setup

**Unity Catalog not available:**
Ensure workspace is Premium tier. Standard tier does not support UC.

**ADLS access denied:**
Verify Access Connector has `Storage Blob Data Contributor` role on the storage account, not just the container.

**Bundle validate fails with state_path error:**
Update `state_path` in `databricks.yml` to include your exact username (copy from workspace URL).

**Kafka connection refused:**
Verify bootstrap server URL includes port `:9092`. Verify API key has `GlobalAccess` scope, not topic-specific.

**DLT pipeline fails on start:**
Ensure `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` Maven library is added to the cluster policy used by the DLT pipeline.

---

*Last updated: April 2026*
