
![Modern Azure Data Platform Architecture](./assets/architecture.png)


## Modern Azure Data Platform | ADF, Databricks, Synapse & Power BI Sales Analytics

### Project Overview

This project demonstrates the development of an **end to end Azure data engineering and analytics solution** using the Microsoft AdventureWorks dataset.

The objective was to build a modern data platform that extracts transactional data from an on premises SQL Server environment, processes and transforms it through a **Medallion Architecture**, and delivers curated business data for analytics and reporting in **Power BI**.

The solution uses:

* **Azure Data Factory**
* **Azure Data Lake Storage Gen2**
* **Azure Databricks**
* **Azure Synapse Analytics**
* **Power BI**
* **Microsoft Entra ID**
* **Azure Key Vault**
The final output is an interactive **Sales Overview Dashboard**, providing visibility into sales performance, product information, customer demographics, and product categories.

### Business Requirements
The business requires a centralized analytics solution capable of transforming operational sales data into meaningful and accessible business insights.

The solution is designed to:
1. Ingest sales, customer, product, and related transactional data from SQL Server.
2. Preserve raw source data for traceability and reprocessing.
3. Clean, standardize, and validate data before analytical use.
4. Create curated datasets containing business ready sales and customer information.
5. Provide analytical access to transformed data.
6. Deliver an interactive Power BI dashboard displaying key metrics including **total sales, number of products, customer distribution, and product categories**.
7. Enable users to dynamically explore the data by **customer title, gender, and product category**.
8. Secure credentials and platform access using centralized identity and secrets management.

### Architecture Overview

The solution follows a layered Azure data architecture:

**On Premises SQL Server → Azure Data Factory → Azure Data Lake Storage Gen2 → Azure Databricks → Azure Synapse Analytics → Power BI**

**Azure Data Factory**: Handles data ingestion and orchestration, moving AdventureWorks transactional data from the source SQL Server database into Azure. </br>
**Azure Data Lake Storage Gen2**: Provides scalable and durable storage for the ingested source data.</br>
**Azure Databricks** : Provides the data processing and transformation layer using the **Medallion Architecture**.</br>
**Azure Synapse Analytics**: Provides the analytical serving layer between the curated Gold datasets and the reporting environment.</br>
**Power BI**: Consumes the prepared analytical data to deliver an interactive **Sales Overview Dashboard** for business users.</br>

**Security and Governance**</br>
**Microsoft Entra ID** provides centralized identity and access management.</br>
**Azure Key Vault** securely manages credentials, secrets, and keys used across the platform.</br>

## Dataset: Microsoft AdventureWorks
This project uses the **Microsoft AdventureWorks** sample database, which represents a fictional bicycle manufacturing and retail organization.

AdventureWorks contains interconnected business entities covering:

* Customers
* Customer addresses
* Products
* Product categories
* Sales orders
* Sales order details
* Employees
* Sales territories
* Purchasing
* Inventory

Its relational structure makes it suitable for demonstrating realistic data engineering, ETL, Medallion Architecture, analytics, and BI workflows.

### Data Processing Logic
![Azure Data Factory Pipeline](./assets/adfpipeline.png)
Data is processed through the Medallion Architecture using PySpark and Spark SQL.

**Bronze Layer**<br>
Raw CSV files are ingested from the source and stored in Azure Data Lake Storage without changing the original structure.

**Silver Layer**<br>
PySpark is used to clean and standardize the data by handling null values, removing duplicates, correcting data types, standardizing text fields, validating keys, and checking data quality.

**Gold Layer**<br>
Spark SQL is used to join cleaned datasets and create analytics ready fact and dimension tables such as fact_sales, fact_orders, dim_customer, dim_product, dim_seller, and dim_date.

The final Gold layer is optimized for reporting and analysis in Azure Synapse Analytics and Power BI.
![Power BI Dashboard](./assets/dashboard.gif)
