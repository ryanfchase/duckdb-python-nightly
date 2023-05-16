# duckdb-python

A simple "Hello World" to get started with duckdb for python. 

# Quick Start

The following will seed a persistent duckdb database called `duckdb_python/db/master.duckdb` with 311 request data for 2023 using a CSV file downloaded from data.lacity.org and output the result of a sample SQL query `SELECT * FROM requests limit 1` to the console. 

Perform the following steps from the terminal  
_Tip: If you want to know what each step is doing, hover over it for an explaination_:
* <a href="#" title="Clones this project">git clone git@github.com:edwinjue/duckdb-python.git</a>
* <a href="#" title="Changes to the project's root directory">cd duckdb-python</a>
* <a href="#" title="Sets up a python virtual environment so any dependencies installed will be for this project only">python -m virtualenv venv</a>
* <a href="#" title="Activates the virtual environment created above">source ./venv/bin/activate</a>
* <a href="#" title="Installs all project dependencies">pip install -r requirements.txt</a>
* <a href="#" title="Creates the directory containing your raw .csv data files">mkdir -p duckdb_python/data/csv</a>
* <a href="#" title="Creates the output directory for all generated .parquet files ">mkdir -p duckdb_python/data/parquet</a>
* <a href="#" title="Creates the directory containing the persistent database">mkdir -p duckdb_python/db</a>
* <a href="#" title="Executes the main code">python [main.py](https://github.com/edwinjue/duckdb-python/blob/main/main.py)</a>

# Credit
* This project was based on the following [tutorial](https://marclamberti.com/blog/duckdb-getting-started-for-beginners/) by marclamberti
