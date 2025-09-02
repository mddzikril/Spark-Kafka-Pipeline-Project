# Master Data Management (MDM) to Kafka Data Pipeline

This project simulates a **real-world Master Data Management (MDM) integration scenario**, focusing on the design and implementation of a **batch data pipeline**. The pipeline synchronizes MDM entity records with **Apache Kafka**, enabling potential downstream systems to consume **golden copies** of entity data in a scalable and consistent manner.

---

## Project Overview

Currently, the project is built around a single entity but it may be extended to other possible entities as well.
- Entity: **Accounts**.  
- One **Account** may have multiple **Related Parties**.  
- Each **Related Party** is associated with one **Address**.  

The pipeline demonstrates how data from multiple normalized tables can be joined, transformed, and published as a consolidated golden record to Kafka topics.

---

## Key Features

- **Entity Normalization & Transformation**  
  - Joins related tables (Account, Party, Address) to create complete golden copies.  
  - Applies business rules and schema standardization to ensure high-quality data.  

- **Data Ingestion & Processing**  
  - Built with **Apache Spark** and **PySpark** for scalable ETL.  
  - Simulates real-world master data integration patterns. 

- **Publishing to Kafka**  
  - Transformed golden copies are published to **Kafka topics** for downstream consumption.  
  - Decouples data producers (MDM platform) from consumers, reducing system load.  

- **CI/CD Automation**  
  - Automated pipeline deployment using **Jenkins CI/CD**.  
  - Version control maintained via **GitHub**.  

---

## Tech Stack

- **Python**
- **Pytest**
- **Apache Spark** (PySpark)  
- **Apache Kafka**  
- **Jenkins**  
- **GitHub**  

---

## Future Work

- Extend to multiple entities (e.g., Products, Employees, Facilities).  
- Add real-time streaming ingestion alongside batch processing.  

---
