# Oracle AI Data Platform Workbench Samples

This repository contains a curated collection of sample notebooks demonstrating how to build data pipelines, run machine learning workloads, and integrate AI capabilities using **Oracle AI Data Platform (AIDP) Workbench** — a unified, governed workspace for data engineering, ML, and AI development powered by Apache Spark.

## What is Oracle AI Data Platform Workbench?

Oracle AI Data Platform Workbench is a unified, governed workspace for building, managing, and deploying AI and data-driven solutions. It brings together notebooks, agent development, orchestration, and catalog management in a single collaborative platform — empowering teams to explore data, fine-tune models, and operationalize AI with trust and speed.

[Learn more about AIDP Workbench →](https://www.oracle.com/ai-data-platform/workbench/)

---

## Repository Structure

```
oracle-aidp-samples/
├── getting-started/          # Foundational notebooks for new users
│   ├── Delta_Lake/           # Delta Lake feature walkthroughs
│   └── migration/            # Migrating workloads to AIDP
├── data-engineering/
│   ├── ingestion/            # Connectors and data loading patterns
│   └── transformation/       # Pipeline architectures and table formats
│       ├── liquid-clustering/
│       ├── medallion-lake/
│       ├── scd/
│       └── streaming/
├── ai/
│   ├── agent-flows/          # Agent orchestration and scheduling
│   └── ml-datascience/       # ML, LLM, and AI service integrations
└── shared-utils/             # Reusable utilities and data generators
```

---

## Sample Catalog

### Getting Started

Foundational examples to help you get up and running on AIDP Workbench.

| Notebook | Description |
|---|---|
| [Access ALH Data](getting-started/Access_ALH_Data.ipynb) | Write and query data in Oracle Autonomous AI Lakehouse (ALH) using PySpark `insertInto` and SQL `INSERT` statements with external catalogs. |
| [Access Object Storage Data](getting-started/Access_Object_Storage_Data.ipynb) | Read and write data from OCI Object Storage using direct access, external volumes, and external tables. |
| [Analyse Data Using PySpark](getting-started/Analyse_Data_Using_PySpark.ipynb) | PySpark fundamentals: catalog and schema setup, table creation, data insertion, schema exploration, and matplotlib visualizations. |
| [Analyse Data Using SQL](getting-started/Analyse_Data_Using_SQL.ipynb) | Core SQL operations on AIDP including DataFrame creation, transformations, aggregations, and simple visualizations. |

#### Delta Lake

| Notebook | Description |
|---|---|
| [Use Delta Lake Table](getting-started/Delta_Lake/Use_Delta_Lake_Table.ipynb) | Comprehensive guide covering Delta table operations: updates, merges, time travel, liquid clustering, and vacuuming. |
| [Delta Change Data Feed](getting-started/Delta_Lake/Delta_Change_Feed.ipynb) | Capture row-level changes (inserts, updates, deletes) from Delta tables for CDC, incremental processing, and streaming pipelines. |
| [Handle Schema Evolution](getting-started/Delta_Lake/Handle_Schema_Evolution.ipynb) | Add and evolve columns in Delta tables without rewriting existing data, leveraging automatic schema evolution. |
| [Delta UniForm Tables](getting-started/Delta_Lake/DeltaUniformTables.ipynb) | Create Delta UniForm tables that automatically synchronize Iceberg metadata for cross-format interoperability. |

#### Migration

| Notebook | Description |
|---|---|
| [Migrate Files from Databricks to AIDP](getting-started/migration/databricks_to_aidp/recursive-files-to-aidp.ipynb) | Recursively export notebooks and files from a Databricks workspace to AIDP using the `databricks-sdk` library. |
| [Download from Git to AIDP](getting-started/migration/git_download_and_extract_to_aidp.ipynb) | Download notebooks and files from a Git repository as a ZIP archive and extract them directly into an AIDP workspace volume. |

---

### Data Engineering — Ingestion

Patterns for connecting to and loading data from a wide range of sources.

| Notebook | Description |
|---|---|
| [Read/Write Oracle Ecosystem Connectors](data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors.ipynb) | Connect to Oracle Database, Oracle Exadata, ALH, and ATP with external catalog support and SQL pushdown. |
| [Read/Write External Ecosystem Connectors](data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb) | Read/write operations with Hive Metastore, Microsoft SQL Server, PostgreSQL, and MySQL. |
| [Read-Only Ingestion Connectors](data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb) | Use read-only connectors for MySQL HeatWave, REST APIs, Oracle Fusion BICC, Kafka, and other sources. |
| [Connect Using Custom JDBC Driver](data-engineering/ingestion/Connect_Using_Custom_JDBC_Driver.ipynb) | Integrate custom JDBC drivers (e.g., SQLite, Snowflake) with Spark for connecting to databases not bundled by default. |
| [Execute Oracle ALH SQL](data-engineering/ingestion/Execute%20Oracle%20ALH%20SQL.ipynb) | Execute SQL statements directly against Oracle ALH using the `oracledb` Python package. |
| [Ingest Data Using YAML](data-engineering/ingestion/Ingest_data_using_yaml/Ingest_data_using_YAML.ipynb) | Config-driven ingestion from cloud storage (CSV, JSON) and JDBC sources with schema validation and data quality checks. |
| [Ingest from Multi-Cloud](data-engineering/ingestion/Ingest_from_Multi_Cloud.ipynb) | Ingest data from Azure Data Lake Storage (ADLS) and AWS S3 with proper JAR configuration and credential management. |
| [Ingest into Apache Iceberg (OCI Native)](data-engineering/ingestion/Ingest_into_iceberg_hadoop_catalog_oci_native.ipynb) | End-to-end Apache Iceberg workflow: table creation, querying, schema evolution, time travel, and metadata inspection using OCI native protocol and Hadoop catalog. |
| [Pipe-Delimited File Ingestion](data-engineering/ingestion/PipeDelimited.ipynb) | Read pipe-delimited (`\|`) files from OCI Object Storage and register them as external tables. |
| [Read Excel Files](data-engineering/ingestion/Read_excel_data/read_excel.ipynb) | Read Excel (`.xlsx`) files using the Spark Excel connector and convert them to Spark DataFrames or CSV. |
| [Streaming from OCI Streaming Service](data-engineering/ingestion/Streaming/StreamingFromOCIStreamingService.ipynb) | Consume messages from OCI Streaming (Kafka-compatible) using Spark Structured Streaming with SASL/OAUTHBearer authentication. |
| [Streaming from Volume Path](data-engineering/ingestion/Streaming/StreamingFromVolumePath.ipynb) | Process CSV files from a workspace volume using one-time micro-batch streaming with `Trigger.Once()`. |

---

### Data Engineering — Transformation

Architectural patterns and pipeline templates for data transformation at scale.

#### Medallion Architecture

Implements the Bronze → Silver → Gold lakehouse pattern with data quality checks and aggregations. Industry variants available:

| Notebook | Industry |
|---|---|
| [Education](data-engineering/transformation/medallion-lake/education_medallion_architecture_demo.ipynb) | Education analytics pipeline |
| [Energy](data-engineering/transformation/medallion-lake/energy_medallion_architecture_demo.ipynb) | Energy consumption and reporting |
| [Financial Services](data-engineering/transformation/medallion-lake/financial_services_medallion_architecture_demo.ipynb) | Financial transactions and risk |
| [Healthcare](data-engineering/transformation/medallion-lake/healthcare_medallion_architecture_demo.ipynb) | Patient records and clinical data |
| [Hospitality](data-engineering/transformation/medallion-lake/hospitality_medallion_architecture_demo.ipynb) | Hotel bookings and guest analytics |
| [Insurance](data-engineering/transformation/medallion-lake/insurance_medallion_architecture_demo.ipynb) | Policy and claims processing |
| [Manufacturing](data-engineering/transformation/medallion-lake/manufacturing_medallion_architecture_demo.ipynb) | Production line and quality data |
| [Media](data-engineering/transformation/medallion-lake/media_medallion_architecture_demo.ipynb) | Content engagement and subscriptions |
| [Real Estate](data-engineering/transformation/medallion-lake/real_estate_medallion_architecture_demo.ipynb) | Property listings and transactions |
| [Retail](data-engineering/transformation/medallion-lake/retail_medallion_architecture_demo.ipynb) | Sales, inventory, and customer data |
| [Telecommunications](data-engineering/transformation/medallion-lake/telecommunications_medallion_architecture_demo.ipynb) | Network usage and customer churn |
| [Transportation](data-engineering/transformation/medallion-lake/transportation_medallion_architecture_demo.ipynb) | Logistics and fleet tracking |

#### Delta Liquid Clustering

Demonstrates Delta Lake liquid clustering for automatic query optimization and data layout management. Industry variants available:

| Notebook | Industry |
|---|---|
| [Education](data-engineering/transformation/liquid-clustering/education_delta_liquid_clustering_demo.ipynb) | Student performance analytics with ML prediction |
| [Energy](data-engineering/transformation/liquid-clustering/energy_delta_liquid_clustering_demo.ipynb) | Smart grid monitoring and anomaly detection |
| [Financial Services](data-engineering/transformation/liquid-clustering/financial_services_delta_liquid_clustering_demo.ipynb) | Transaction analytics and reporting |
| [Healthcare](data-engineering/transformation/liquid-clustering/healthcare_delta_liquid_clustering_demo.ipynb) | Patient data access patterns |
| [Hospitality](data-engineering/transformation/liquid-clustering/hospitality_delta_liquid_clustering_demo.ipynb) | Booking and occupancy analytics |
| [Insurance](data-engineering/transformation/liquid-clustering/insurance_delta_liquid_clustering_demo.ipynb) | Claims and policy data optimization |
| [Manufacturing](data-engineering/transformation/liquid-clustering/manufacturing_delta_liquid_clustering_demo.ipynb) | Production and quality metrics |
| [Media](data-engineering/transformation/liquid-clustering/media_delta_liquid_clustering_demo.ipynb) | Content and engagement data |
| [Real Estate](data-engineering/transformation/liquid-clustering/real_estate_delta_liquid_clustering_demo.ipynb) | Property and transaction data |
| [Retail](data-engineering/transformation/liquid-clustering/retail_delta_liquid_clustering_demo.ipynb) | Sales and inventory analytics |
| [Telecommunications](data-engineering/transformation/liquid-clustering/telecommunications_delta_liquid_clustering_demo.ipynb) | Network and customer usage data |
| [Transportation](data-engineering/transformation/liquid-clustering/transportation_delta_liquid_clustering_demo.ipynb) | Fleet and logistics optimization |

#### Apache Iceberg Uniform Liquid Clustering

Combines Delta UniForm with Apache Iceberg Liquid Clustering for open-format, cross-engine table optimization. Industry variants available:

| Notebook | Industry |
|---|---|
| [Education](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/education_iceberg_liquid_clustering_demo.ipynb) | Student performance data |
| [Energy](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/energy_iceberg_liquid_clustering_demo.ipynb) | Grid and sensor data |
| [Financial Services](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/financial_services_iceberg_liquid_clustering_demo.ipynb) | Transaction and risk data |
| [Healthcare](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/healthcare_iceberg_liquid_clustering_demo.ipynb) | Clinical and patient records |
| [Hospitality](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/hospitality_iceberg_liquid_clustering_demo.ipynb) | Booking and revenue data |
| [Insurance](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/insurance_iceberg_liquid_clustering_demo.ipynb) | Policy and claims data |
| [Manufacturing](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/manufacturing_iceberg_liquid_clustering_demo.ipynb) | Production and IoT data |
| [Media](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/media_iceberg_liquid_clustering_demo.ipynb) | Content delivery data |
| [Real Estate](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/real_estate_iceberg_liquid_clustering_demo.ipynb) | Property listings data |
| [Retail](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/retail_iceberg_liquid_clustering_demo.ipynb) | Sales and inventory data |
| [Telecommunications](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/telecommunications_iceberg_liquid_clustering_demo.ipynb) | Network usage data |
| [Transportation](data-engineering/transformation/liquid-clustering/iceberg_uniform_liquid_clustering/transportation_iceberg_liquid_clustering_demo.ipynb) | Fleet and route data |

#### Other Transformation Patterns

| Notebook | Description |
|---|---|
| [Slowly Changing Dimensions (SCD Type 2)](data-engineering/transformation/scd/slowly_changing_dimension_template.ipynb) | Track historical changes to dimension records using SCD Type 2 with Jinja2-templated merge logic. |
| [Streaming — Energy Delta Liquid Clustering](data-engineering/transformation/streaming/energy_delta_streaming_liquid_clustering_demo.ipynb) | Real-time smart grid monitoring with streaming Delta tables, anomaly detection, and statistical baselines for energy consumption. |
| [Streaming — Manufacturing Delta Liquid Clustering](data-engineering/transformation/streaming/manufacturing_delta_streaming_liquid_clustering_demo.ipynb) | Continuous ingestion and clustering of manufacturing sensor data using Spark Structured Streaming and Delta Lake. |

---

### AI & Machine Learning

Notebooks covering generative AI, NLP, ML model training, and LLM-powered analytics.

| Notebook | Description |
|---|---|
| [Sentiment Analysis with OCI GenAI](ai/ml-datascience/ai-services/OCI_Gen_AI_Sentiment_Analysis.ipynb) | Perform sentiment analysis on text data using OCI Generative AI (Llama model) via the AIDP `query_model` function. |
| [OCI Language Service Translation](ai/ml-datascience/ai-services/OCI_LanguageService_Example.ipynb) | Translate text using OCI AI Language Service via REST API, demonstrated with a round-trip English ↔ Spanish translation. |
| [Customer Churn Prediction (GPU)](ai/ml-datascience/churn-analysis/CustomerChurn_ModelTraining_GPU.ipynb) | Train a TensorFlow neural network on GPU for customer churn prediction, including preprocessing, training, and evaluation. |
| [LLM Model Output Parser](ai/ml-datascience/llm-utils/llm_model_output_parser.ipynb) | Use an LLM to parse and translate statistical model outputs into business-friendly insights and plain-language summaries. |
| [Natural Language to SQL (NL2SQL)](ai/ml-datascience/nl2sql/nl2sql.ipynb) | Introspect a database schema and generate accurate SQL queries from natural language questions using an LLM, with result summarization. |
| [Multi-Table NL2SQL with Grouped Analysis](ai/ml-datascience/nl2sql/multi_table_nl2sql_analysis.ipynb) | Extend NL2SQL to multi-table scenarios with grouped LLM analysis for complex procurement and supplier-item intelligence. |
| [Retrieval-Augmented Generation (RAG)](ai/ml-datascience/rag/rag_implementation.ipynb) | End-to-end RAG pipeline: ingest documents from OCI Object Storage, chunk and embed text, retrieve relevant context, and generate answers with an LLM. |
| [Movie Recommendation System](ai/ml-datascience/recommendation-systems/Pyspark_ML_Movie_Recommendation.ipynb) | Build a collaborative filtering recommendation engine using PySpark ML's ALS algorithm, trained and evaluated on movie rating data. |
| [Linear Mixed Effects Model](ai/ml-datascience/statistical-modeling/Statsmodels_LME_Demo.ipynb) | Apply a Linear Mixed Effects Model (LME) with `statsmodels` and PySpark to analyze student test scores across schools, accounting for fixed and random effects. |

#### Agent Flows

| Notebook | Description |
|---|---|
| [Agent Flow Schedule Trigger](ai/agent-flows/misc/agent-flow-schedule-trigger/task_notebook.ipynb) | Invoke AIDP agent flows via REST API using OCI request signing, demonstrating programmatic agent orchestration with custom message handling. |

---

### Shared Utilities

| Notebook | Description |
|---|---|
| [Data Code Generator](shared-utils/data_generator/data_code_generator_Example.ipynb) | Generate realistic multi-table synthetic datasets from a YAML configuration file, with CSV and JSON export support for testing and prototyping. |

---

## Running the Samples

### Prerequisites

Before running any sample, ensure you have:

- An active **Oracle AI Data Platform Workbench** environment with a compute cluster.
- The required **IAM policies** configured for the services used (Object Storage, ALH, AI Services, etc.).
- Cluster libraries installed from the `requirements.txt` file included in the relevant sample folder, where applicable.

### General Steps

1. Open your AIDP Workbench notebook environment.
2. Clone or import the samples into your workspace.
3. Navigate to the notebook of your choice and open it.
4. Follow the instructions and prerequisites described in the notebook's opening cells.
5. Attach the notebook to a running compute cluster and execute the cells.

### MLflow Tracking Server

Several ML samples integrate with **MLflow** for experiment tracking. Ensure your AIDP environment has an MLflow Tracking Server configured. Refer to the AIDP documentation for setup instructions.

---

## Documentation

- [Oracle AI Data Platform Workbench](https://www.oracle.com/ai-data-platform/workbench/)
- [OCI Documentation](https://docs.oracle.com/en-us/iaas/Content/home.htm)

---

## Get Support

If you encounter issues with these samples, please open an issue in this repository. For questions about Oracle AI Data Platform itself, refer to the [OCI Support](https://www.oracle.com/support/) portal.

---

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process.

---

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md).

---

## License

See [LICENSE](./LICENSE.txt)
