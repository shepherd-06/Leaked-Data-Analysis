# A Data Pipeline & Analysis Project for Malware-Compromised Credential Data

It is a full data-ingestion and analysis workflow built to study a large dataset of compromised credentials collected from malware-infected systems.
The project focuses on building a reproducible ETL pipeline, loading the raw JSON dataset into a structured PostgreSQL database using Python, and preparing the cleaned data for Tableau and Power BI visualization.

This repository contains:

- A PostgreSQL schema tailored for the dataset
- A Python ingestion pipeline that imports hundreds of thousands of JSON files safely
- Automatic file-processing tracking to avoid duplicates
- Scripts for validating table creation and database health
- Data-cleaning steps
- Instructions for exporting final data for Tableau & Power BI dashboards

## Dataset Source & Attribution

All raw JSON data used in this project comes from the paper:

Malware Finances and Operations: a Data-Driven Study of the Value Chain for Infections and Compromised Access

Creators:

- Juha Nurmi
- Mikko Niemelä
- Billy Brumley

Dataset URL: https://zenodo.org/records/8047205

This dataset is not created or owned by this project.
All rights belong to the authors.
This repository only contains scripts for local parsing and analysis.

## Purpose

The goal of this project is to analyze how malware-infected devices leak sensitive information—such as:

- stored credentials
- domain names
- services accessed
- usernames
- emails
- IP address metadata

and to derive patterns, statistics, and correlations using data visualization tools.

Specifically, this project aims to:

1. Convert raw JSON files into structured relational tables
2. Enable large-scale querying inside PostgreSQL
3. Perform exploratory analysis in Python
4. Export clean datasets for visualization
5. Build Tableau & Power BI dashboards to reveal trends such as:
    - geographic distribution of infections
    - frequency of password reuse
    - relationships between services, domains, and credentials
    - temporal patterns in leaks