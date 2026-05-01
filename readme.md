# **Walmart Near-Real-Time Data Warehouse (DW) Prototype – README**



## **Project Overview**



This project implements a near-real-time Data Warehouse for Walmart. It processes large transactional data streams, joins them with master data (customers, products, stores, suppliers), and loads the results into a star-schema DW for analysis.



The project uses:



**Python** – for ETL and HYBRIDJOIN stream processing



**MySQL** – for DW storage and analytics





#### Prerequisites



1. **Python (>=3.8 recommended) with the following libraries:**



* mysql-connector-python
* csv
* threading
* collections
* datetime



2\. **MySQL Database installed locally or on a server.**



3\. **CSV Files for master data and transactional data:**



* customer\_master\_data.csv
* product\_master\_data.csv
* transactional\_data.csv



Sufficient memory for buffers and partitions (stream buffer: 10,000 slots; product partition size: 500).





#### Step-by-Step Instructions



##### Step 1: Setup the Database

1. Open MySQL Workbench or any MySQL client.

2\. Execute the provided SQL script walmart\_dw\_v2.sql to:

* Create database walmart\_dw\_v2
* Create dimensions (customer\_dim\_v2, product\_dim\_v2, store\_dim\_v2, supplier\_dim\_v2, date\_dim\_v2)
* Create fact table (sales\_fact\_v2) with all foreign keys and indexes



##### Step 2: Prepare CSV Files



1. Place the CSV files in accessible locations on your machine.

2\. Update the Python script with the correct file paths:

CUSTOMER\_CSV = r"C://Users//Shaban Hassan//OneDrive//Desktop//warehouse//customer\_master\_data.csv"

PRODUCT\_CSV  = r"C://Users//Shaban Hassan//OneDrive//Desktop//warehouse//product\_master\_data.csv"

TRANS\_CSV    = r"C://Users//Shaban Hassan//OneDrive//Desktop//warehouse//transactional\_data.csv"



##### Step 3: Install Python Dependencies



Run the following command to install MySQL connector:

**pip install mysql-connector-python**



##### **Step 4:** Run the Python ETL Script



Run the hybrid.py script (the python code)



:**The script will prompt for database connection details:**



* Host (default: localhost)
* Port (default: 3306)
* Database name: walmart\_dw\_v2
* User and password



**:The script executes the following steps:**

* Loads product and customer master data into memory
* Reads transactional data stream and buffers it
* Performs HYBRIDJOIN to join transactions with master data
* Inserts the joined data into the DW tables (\*\_dim\_v2 and sales\_fact\_v2)
* Handles duplicates automatically for dimensions



**:Progress messages will display:**

* Number of stream rows loaded
* Batch processing updates
* Completed joins (join applied on all approx. 550000 records same as of transactional.csv)



##### Step 5: Run 20 OLAP Analytics Queries ON SQL SCRIPT 





##### Step 6: Handling Errors (THE DUPLICATES)

**1. Customer Dimension (customer\_dim\_v2)**

**Mechanism:**

* Before inserting new customer records, the script checks if the Customer\_ID already exists in the database.
* Uses a dictionary (unique\_customers) to store only unique customer records from the batch.
* Existing customers are retrieved via:

&nbsp;	SELECT customer\_id, customer\_sk FROM customer\_dim\_v2 WHERE customer\_id IN (...)

Only new customers are inserted using INSERT statements.



**Result:**

* Prevents duplicate entries for the same Customer\_ID.
* Ensures that the surrogate key (customer\_sk) is consistent for repeated customers.



**2.Date Dimension (date\_dim\_v2)**

**Mechanism:**

* **Uses a set to identify unique dates from the transactional batch:using**
* 
**&nbsp;	unique\_dates = list(set(date\_list))**

* **Checks the database for existing dates.**
* **Inserts only new dates with INSERT IGNORE.**



**Result:**

* **No duplicate date entries.**
* **Surrogate key (date\_sk) remains consistent.**



**3.Fact Table (sales\_fact\_v2)**

**Mechanism:**

* Each transaction is uniquely identified by its orderID.
* Fact records reference surrogate keys of dimension tables.
* Because the dimensions are deduplicated first, the fact table only contains valid, unique references.



**Result:**

Duplicate transactions are avoided as the script ensures each orderID is inserted once per batch.



#### Overall Conclusion

This project demonstrates the design and implementation of a near-real-time Data Warehouse for Walmart using a HYBRIDJOIN streaming ETL approach. By combining in-memory hash tables, stream buffers, and partitioned master data, the system efficiently processes large transactional datasets while maintaining data integrity and preventing duplicates.









