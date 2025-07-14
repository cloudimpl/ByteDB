#!/bin/bash

echo "ByteDB Catalog Modes Example"
echo "============================"
echo

echo "1. In-Memory Catalog (Default)"
echo "------------------------------"
echo "./bytedb"
echo "  - Tables exist only during the session"
echo "  - Perfect for ad-hoc analysis"
echo "  - No cleanup needed"
echo
echo "Example:"
echo "  bytedb> SELECT COUNT(*) FROM read_parquet('sales_2024.parquet');"
echo "  bytedb> CREATE TABLE sales AS SELECT * FROM read_parquet('sales_2024.parquet');"
echo "  bytedb> SELECT region, SUM(amount) FROM sales GROUP BY region;"
echo "  bytedb> exit  # Table 'sales' is lost"
echo

echo "2. Persistent Catalog"
echo "--------------------"
echo "./bytedb --persist"
echo "./bytedb -p /data/my_catalog"
echo "  - Tables persist between sessions"
echo "  - Good for repeated analysis"
echo "  - Catalog stored in .catalog directory"
echo
echo "Example:"
echo "  # First session"
echo "  bytedb> CREATE TABLE customers AS SELECT * FROM read_parquet('customers.parquet');"
echo "  bytedb> exit"
echo "  "
echo "  # Later session"
echo "  bytedb> SELECT * FROM customers WHERE country = 'USA';  # Table still exists!"
echo

echo "3. Direct File Queries (No Catalog Needed)"
echo "-----------------------------------------"
echo "Works in both modes:"
echo "  bytedb> SELECT * FROM read_parquet('path/to/any/file.parquet');"
echo "  bytedb> SELECT * FROM read_parquet('/absolute/path/data.parquet');"
echo "  bytedb> SELECT a.*, b.name"
echo "          FROM read_parquet('orders.parquet') a"
echo "          JOIN read_parquet('customers.parquet') b ON a.customer_id = b.id;"