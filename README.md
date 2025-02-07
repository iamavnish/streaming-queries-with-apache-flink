# Queries on Streaming Data with Managed Apache Flink 

## Overview
This is Proof of Concept for processing streaming Data with Managed Apache Flink.

## PoC Compleity Level
Medium

## Tech Stack
- PyFlink
- Amazon Managed Service for Apache Flink
- Amazon Kinesis Data Stream

## Use Case / Problem Statement
An Amazon Kinesis Data Stream which is acting as Producer is continuously generating data related to orders made by customers on the website of a business. The business team is eager to gain real time insights into the behavior of Customers on their website like user buying habits, preferences and other business metrics as they happen. So, they want to process and analyze this data stream (of orders) providing them with valuable, actionable insights in real-time.

## Dataset
Order data in JSON format. Here is a sample record:

{
    'order_id': '1f4da8b2-73d0-49d5-9762-3e2e0a3cf004', 
    'order_timestamp': '2024-04-04T15:32:03', 
    'order_date': '2024-04-04', 
    'customer_number': 198, 
    'customer_visit_number': 1,
    'customer_city': 'Makati City', 
    'customer_country': 'Philippines',
    'customer_credit_limit': 60237,
    'device_type': 'desktop', 
    'browser': 'Opera/9.32.(Windows 98; tig-ER) Presto/2.9.179 Version/10.00', 
    'operating_system': 'Android', 
    'product_code': 'S32_1268', 
    'product_line': 'Trucks and Buses',
    'product_unitary_price': 96.31, 
    'quantity': 10, 
    'total_price': 963.1,
    'traffic_source': 'www.hardin-green.com'
}

## Solution Architecture
![image](https://github.com/user-attachments/assets/10e6320f-09c6-4267-b122-5ceff2c4b4cc)

Here, PyFlink which allows to write Flink applications in Python, is used to process streaming data. Flink is structured around two major APIs: the DataStream API and the Table API and SQL. The DataStream API is a low-level API while the Table API and SQL offers a more declarative approach, making it easier to write complex data transformations and aggregations.

The core element of the Table API & SQL is the entity known as a Table. A table can connect to a variety of source or target systems such as databases, files, message queues or data streams. It doesn't contain the data in itself. Instead, it describes how to read data from a source and how to eventually write data to a sink. Table is defined with schema of the dataset that is expected to arrive from the data source along with connection details. After tables are established, a wide range of operations such as filtering, aggregating, joining etc can be performed over them. These operations can be expressed using either the Table API, which provides a programmatic way to manipulate tables or SQL queries.


