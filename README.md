# Elmar Zeynalov

**Big Data & Distributed Systems Engineer** · **M.Sc. in Cloud & Network Infrastructures (SABAH Group)**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-elmarzeynalov-0A66C2?style=flat-square&logo=linkedin)](https://www.linkedin.com/in/elmarzeynalov)
[![Email](https://img.shields.io/badge/Email-elmar.q.zeynalov%40gmail.com-EA4335?style=flat-square&logo=gmail)](mailto:elmar.q.zeynalov@gmail.com)
[![Astronomer Certified](https://img.shields.io/badge/Astronomer_Certified-Apache_Airflow_3-017CEE?style=flat-square&logo=apacheairflow)](https://www.credly.com/go/7uOJAmbR)
[![DataEng.AZ](https://img.shields.io/badge/Live_Platform-DataEng.AZ-000000?style=flat-square&logo=vercel)](https://project-c037y.vercel.app)

IT graduate (**B.Sc., SABAH Group, GPA: 86/100**) and current **M.Sc. researcher in Cloud and Network Infrastructures** at Azerbaijan Technical University. Focused on engineering **distributed data pipelines**, **high-frequency real-time streaming systems**, and **multi-tier Lakehouse & Data Warehouse architectures** on containerized Linux environments.

---

## Technical Stack & Engineering Domains

| Domain | Technologies & Engineering Practices |
| :--- | :--- |
| **Big Data & Stream Processing** | `Apache Spark (PySpark)` · `Spark Structured Streaming` · `Apache Kafka (KRaft, Pub/Sub)` · `Micro-Batch & High-Frequency Ingestion` |
| **Workflow Orchestration** | `Apache Airflow (DAGs, TaskGroups, SqlSensor, FileSensor, PythonSensor, Hooks)` |
| **Data Architecture & Modeling** | `Medallion Architecture (Bronze / Silver / Gold)` · `Kimball Star Schema` · `SCD Type 1 & Type 2` · `Junk Dimensions` · `Automated Data Quality Gates` |
| **Databases & Lakehouse Storage** | `PostgreSQL (FDW, JDBC)` · `Oracle SQL / PL/SQL` · `MS SQL Server (T-SQL)` · `MinIO (S3A Object Storage)` · `Snappy Parquet` · `Apache Iceberg` |
| **Programming & Backend** | `Python (PySpark, Pandas, NumPy, FastAPI)` · `Advanced SQL (CTEs, Window Functions, Query Optimization)` · `Bash` · `OOP & Data Structures` |
| **Infrastructure & Observability** | `Docker & Docker Compose` · `Linux (Ubuntu/Debian CLI)` · `Computer Networks (TCP/IP, DNS, DHCP)` · `Grafana` · `Metabase` · `Streamlit` |

---

## Featured Engineering Projects

### 1. [Real-Time High-Frequency Cryptocurrency Streaming Pipeline](https://github.com/zeynalovvelmar/crypto-realtime-data-pipeline)
> **Architecture:** `CoinGecko REST API` → `Python Producer (acks='all')` → `Apache Kafka 7.7.8 (KRaft, 3 Partitions)` → `PySpark Structured Streaming (10s Trigger, foreachBatch)` → `PostgreSQL 16` → `Grafana 10.4`
- Deployed a ZooKeeper-less **Kafka cluster in KRaft mode** with deterministic partition routing (`key = coin_id`) and `acks="all"` delivery durability across 10 market assets.
- Engineered a fault-tolerant **PySpark Structured Streaming** consumer enforcing strict `StructType` schema parsing, checkpoint recovery, and JDBC micro-batch persistence to PostgreSQL, powering live **Grafana** throughput and price trajectory dashboards.

### 2. [Enterprise Banking Data Lakehouse — 3-Tier Medallion Batch Architecture](https://github.com/zeynalovvelmar/bank_batch_pipeline)
> **Architecture:** `PostgreSQL 16 (9 OLTP Tables)` → `PySpark JDBC` → `MinIO S3A (Bronze → Silver → Gold Parquet)` → `Apache Airflow 2.10` → `Streamlit & PySpark SQL`
- Architected an idempotent 8-container Lakehouse pipeline extracting 9 relational banking tables into **MinIO (S3A)** partitioned Parquet storage.
- Implemented **5 automated Data Quality Gates** in the Silver layer (resolving duplicate PKs, null amounts, negative sign anomalies, invalid ISO currencies, and orphan foreign keys) and modeled a **Kimball Star Schema** in the Gold layer with **SCD Type 2** time-aware historical joins (`dim_customer` + `fact_transaction`).

### 3. [Automated Credit Risk Decision Scoring & Workflow Orchestration](https://github.com/zeynalovvelmar/credit-risk-etl-airflow)
> **Architecture:** `SqlSensor & FileSensor (tg_precheck)` → `MinIO S3 & PySpark Scoring (tg_ingestion_processing)` → `Snappy Parquet & Daily CSV Reports (tg_reporting)`
- Orchestrated a modular **Apache Airflow** pipeline using `TaskGroups`, `SqlSensor`, and `FileSensor` to guarantee upstream PostgreSQL and flat-file readiness before execution.
- Implemented deterministic **PySpark** underwriting logic evaluating blacklist membership, debt-to-income (DTI > 70%) ratios, income multiples, and employment tenure to classify applications and publish daily exposure summaries to **MinIO**.

### 4. [Multi-Layer Retail Data Warehouse & BI Pipeline (`Staging → DWH → Data Mart`)](https://github.com/zeynalovvelmar/data-engineering-pipeline)
> **Architecture:** `Source OLTP PostgreSQL` → `postgres_fdw (ext_source)` → `Staging (Full & Incremental Upserts)` → `Core DWH (Star Schema)` → `Analytical Data Marts` → `Metabase BI`
- Designed a 3-schema warehouse supporting both **Full Reload** and **Incremental (`ON CONFLICT DO UPDATE`)** synchronization orchestrated across 4 **Apache Airflow** DAGs with `{{ run_id }}` batch lineage and **Metabase** serving.

### 5. [DataEng.AZ — Interactive Data Engineering Learning & Sandbox Platform](https://github.com/zeynalovvelmar/de-learning-platform) · [[Live Demo ↗]](https://project-c037y.vercel.app)
> **Stack:** `Python` · `FastAPI` · `Next.js 16 (React 19, TypeScript)` · `SQLite In-Memory Sandbox` · `102 SVG Architecture Diagrams`
- Built a bilingual (**EN / AZ**) interactive engineering curriculum featuring **10 modules (102 lessons)** across SQL, Python, Spark, Kafka, Airflow, dbt, Docker, and Open Lakehouse (Iceberg/Nessie/Trino), complete with live **SQL & Python code execution sandboxes**.

### 6. [Additional Data Engineering & Modeling Repositories](https://github.com/zeynalovvelmar?tab=repositories)
- **[E-Commerce Star Schema ELT Pipeline (Oracle SQL)](https://github.com/zeynalovvelmar/ecommerce-elt-pipeline):** Transforms 36-column unstructured retail extracts into a Kimball Star Schema with identity surrogate keys, a consolidated **Junk Dimension** (`dim_junk`), and `NVL`-safe composite joins.
- **[FuelCo Daily Telemetry ETL & Data Mart Pipeline](https://github.com/zeynalovvelmar/fuelco-pipeline):** Airflow pipeline (`PythonSensor` in `reschedule` mode + `PostgresHook`) ingesting multi-station fuel sales with post-load data quality validation.
- **[Hospital Management System 3NF Database Design](https://github.com/zeynalovvelmar/hospital-management-system-db):** Conceptual & Logical ERDs and PostgreSQL DDL with referential integrity and B-Tree indexing.

---

## Education & Certifications

- **M.Sc. in Cloud and Network Infrastructures (SABAH Group)** — *Azerbaijan Technical University* (2026 – Present)
- **B.Sc. in Information Technology (SABAH Group, GPA: 86/100)** — *Azerbaijan Technical University* (2022 – 2026)
- **Data Engineering Intensive Bootcamp (Diploma ID: M82201370)** — *Matrix Academy* (Dec 2025 – Jun 2026)
- **[Astronomer Certification for Apache Airflow 3 Fundamentals](https://www.credly.com/go/7uOJAmbR)** — *Astronomer* (Apr 2026)
- **Introduction to Entrepreneurship & Analytical Problem Solving (ID: SA-1001112)** — *SABAH.academy & IE University* (Jan 2026)
