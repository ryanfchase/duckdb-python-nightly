# duckdb-python

A simple "Hello World" to get started with duckdb for python. 

# Quick Start

The following will seed a persistent duckdb database called `duckdb_python/db/master.duckdb` with 311 request data for 2023 using a CSV file downloaded from data.lacity.org and output the result of a sample SQL query `SELECT * FROM requests limit 1` to the console. 

Perform the following steps from the terminal:
* git clone git@github.com:edwinjue/duckdb-python.git
* cd duckdb-python
* pip install -r requirements.txt
* mkdir -p duckdb_python/data/csv
* mkdir -p duckdb_python/data/parquet
* mkdir -p duckdb_python/db
* python main.py

# Special Thanks
* https://marclamberti.com/blog/duckdb-getting-started-for-beginners/
