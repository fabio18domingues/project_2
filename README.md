# project_2

# Football Data Platform – Azure Fabric & Databricks

## Overview
This project is an end-to-end cloud data engineering platform designed to ingest, process, and analyze football data across multiple leagues and seasons.

The solution follows enterprise-grade data engineering best practices, using Azure-native services and Microsoft Fabric to deliver a scalable, governed, and analytics-ready data platform.

This project was built as a portfolio project to demonstrate real-world data engineering skills applicable to large-scale cloud environments.

---

## Architecture Overview
The platform follows a modern Lakehouse architecture with Bronze, Silver, and Gold layers.

![Architecture](architecture/architecture-overview.png)

---

## Technology Stack
- **Azure Blob Storage** – Raw data ingestion
- **Azure Data Factory** – Orchestration and control-table-driven ingestion
- **Microsoft Fabric Lakehouse (OneLake)** – Centralized data lake
- **Databricks** – Delta Lake processing
- **Power BI** – Analytics and visualization
- **Delta Lake** – ACID tables and scalable storage

---

## Data Ingestion
- Data is organized by league and season
- Azure Data Factory dynamically ingests data based on a control table
- Supports **FULL** and **INCREMENTAL** loads
- File discovery is fully automated
- Metadata-driven ingestion (no hardcoded paths)

---

## Data Processing
The solution follows the **Medallion Architecture**:

### Bronze Layer
- Raw CSV files ingested into OneLake
- Converted to Delta format
- Preserves original data structure

### Silver Layer
- Data cleansing and normalization
- Schema standardization
- Business-friendly naming
- Data quality rules applied

### Gold Layer
- Analytical fact and dimension tables
- Optimized for reporting and analytics
- Aggregations by league, season, and player

---

## Analytics & Reporting
- Power BI dashboards built on Gold layer
- Player performance metrics:
  - Goals
  - Shots
  - Cards
- League and season comparisons
- Snowflake schema data model
- Designed for Direct Lake access

---

## Key Features
- Cloud-native architecture
- Enterprise-ready ingestion framework
- Delta Lake best practices
- Scalable and modular design
- Suitable for large datasets and future expansion

---

## Disclaimer
This repository does not display any credentials.
