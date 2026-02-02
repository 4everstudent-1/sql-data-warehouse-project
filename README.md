# First data project - Warehouse and Analytics 

Welcome to my first repository!

This project is the result of completing the Udemy course “The Complete SQL Bootcamp” by DataWithBarra. It serves as a hands-on application of both the theoretical and practical SQL knowledge I gained, focused on building a data warehouse and generating actionable insights from the data.

***

### License

This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.

### About Me

Hi there! I’m an IT professional currently working as an Enterprise Support Engineer for a large SaaS company. I’m passionate about data and data manipulation, and this project represents my first step into the data world. I’m excited to continue building my knowledge and to create a similar repository of my own in the future.

Thank you for stopping by!

***
This is a copy of the original repository, which can be found here: https://github.com/DataWithBaraa/sql-data-warehouse-project/tree/main 

***

### Data Architecture

The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers: Data Architecture

<img width="6235" height="3216" alt="data_architecture" src="https://github.com/user-attachments/assets/84a83aa2-a27f-4476-9fe2-8c6328a48e8a" />


Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

### Project Overview

This project involves:

    Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
    ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
    Data Modeling: Developing fact and dimension tables optimized for analytical queries.
    Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.


### Project Requirements

### Building the Data Warehouse (Data Engineering)

### Objective

Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

### Specifications

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files. **
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis. **
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries. **
- **Scope**: Focus on the latest dataset only; historization of data is not required. **
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams. **


### BI: Analytics & Reporting (Data Analysis)

Objective

Develop SQL-based analytics to deliver detailed insights into:

    Customer Behavior
    Product Performance
    Sales Trends

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

For more details, refer to docs/requirements.md.

###  Repository Structure

```text
data-warehouse-project/
├── datasets/                # Raw datasets used for the project (ERP and CRM data)
├── docs/                    # Project documentation and architecture details
│   ├── etl.drawio
│   ├── data_architecture.drawio
│   ├── data_catalog.md
│   ├── data_flow.drawio
│   ├── data_models.drawio
│   └── naming_conventions.md
├── scripts/                 # SQL scripts for ETL and transformations
│   ├── bronze/              # Extracting and loading raw data
│   ├── silver/              # Cleaning and transforming data
│   └── gold/                # Analytical models
├── tests/                   # Test scripts and quality files
├── README.md                # Project overview and instructions
├── LICENSE                  # License information
├── .gitignore               # Git ignored files
└── requirements.txt         # Project dependencies
```


### Objective

Develop SQL-based analytics to deliver detailed insights into:

- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

