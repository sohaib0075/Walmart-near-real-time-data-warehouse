# Walmart Near-Real-Time Data Warehouse

## 📌 Overview
This project implements a near-real-time Data Warehouse prototype using **Python and MySQL**. It processes large transactional data streams and integrates them with master data using a **HYBRIDJOIN-based ETL pipeline**, loading results into a **star schema** for efficient analytics and OLAP queries.

---

## ⚙️ Tech Stack
- Python (ETL & Stream Processing)
- MySQL (Data Warehouse)
- CSV (Data Sources)

---

## 🏗️ Architecture
- **Dimensions**: Customer, Product, Store, Supplier, Date  
- **Fact Table**: Sales  
- **Schema**: Star Schema  
- **ETL Type**: Near Real-Time Streaming  

---

## 🔄 Key Features
- HYBRIDJOIN algorithm for efficient stream joins  
- Handles large transactional datasets (~500k+ records)  
- Duplicate handling for dimensions and fact table  
- Batch processing with buffering  
- OLAP-ready schema for analytics  

---

## 📂 Project Structure

warehouse_project/
├── hybrid.py
├── db.sql
├── customer_master_data.csv
├── product_master_data.csv
├── README.md
├── report.docx


---

## 🚀 Setup Instructions

### 1. Setup Database
- Open MySQL Workbench  
- Run `db.sql` to create schema and tables  

### 2. Install Dependencies
```bash
pip install mysql-connector-python
3. Configure File Paths

Update paths in hybrid.py:

CUSTOMER_CSV = "customer_master_data.csv"
PRODUCT_CSV = "product_master_data.csv"
TRANS_CSV = "transactional_data.csv"
4. Run ETL Script
python hybrid.py
📊 Output
Populates dimension tables
Loads fact table with joined data
Supports OLAP queries for insights
🧠 Learning Outcomes
Data Warehouse design (Star Schema)
Stream processing with HYBRIDJOIN
Python + SQL integration
Handling large-scale data efficiently
