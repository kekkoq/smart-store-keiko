#  PROJECT OVERVIEW

This project was developed as part of a course on data-driven decision making, with a focus on building scalable analytics workflows using modern data tools and architectures. The goal is to simulate a real-world business scenario where raw operational data must be transformed into actionable insights through a structured pipeline.
The project emphasizes key concepts in:

- ETL (Extract, Transform, Load): Designing repeatable data ingestion and transformation flows
- Data Warehousing: Structuring data for efficient querying and historical analysis
- OLAP (Online Analytical Processing): Enabling multidimensional analysis for strategic decision support
- Power BI: Visualizing KPIs and trends to inform business stakeholders
- Apache Spark: Exploring distributed data processing for large-scale transformation tasks

It introduces reproducible environment management using uv, ensuring consistency across development and deployment.

- Additional information: <https://github.com/kekkoq/smart-store-keiko>
- Project organization: [STRUCTURE](./STRUCTURE.md)
- Build professional skills:
  - **Environment Management**: Every project in isolation
  - **Code Quality**: Automated checks for fewer bugs
  - **Documentation**: Use modern project documentation tools
  - **Testing**: Prove your code works
  - **Version Control**: Collaborate professionally

---

## WORKFLOW 1. Set Up Your Machine

Proper setup is critical.
Complete each step in the following guide and verify carefully.

- [SET UP MACHINE](./SET_UP_MACHINE.md)

---

## WORKFLOW 2. Set Up Your Project

After verifying your machine is set up, set up a new Python project by copying this template.
Complete each step in the following guide.

- [SET UP PROJECT](./SET_UP_PROJECT.md)

It includes the critical commands to set up your local environment (and activate it):

```shell
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```

**Windows (PowerShell):**

```shell
.\.venv\Scripts\activate
```

**macOS / Linux / WSL:**

```shell
source .venv/bin/activate
```

---

## WORKFLOW 3. Daily Workflow

Please ensure that the prior steps have been verified before continuing.
When working on a project, we open just that project in VS Code.

### 3.1 Git Pull from GitHub

Always start with `git pull` to check for any changes made to the GitHub repo.

```shell
git pull
```

### 3.2 Run Checks as You Work

This mirrors real work where we typically:

1. Update dependencies (for security and compatibility).
2. Clean unused cached packages to free space.
3. Clean up old log files to prevent clutter and keep recent context.
4. Use `git add .` to stage all changes.
5. Run ruff and fix minor issues.
6. Update pre-commit periodically.
7. Run pre-commit quality checks on all code files (**twice if needed**, the first pass may fix things).
8. Run tests.

In VS Code, open your repository, then open a terminal (Terminal / New Terminal) and run the following commands one at a time to check the code.

```shell
uv sync --extra dev --extra docs --upgrade
uv cache clean
uv run python scripts/cleanup_log.py
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
git add .
uv run pytest
```

NOTE: The second `git add .` ensures any automatic fixes made by Ruff or pre-commit are included before testing or committing.

> **Log Cleanup:** `cleanup_log.py` deletes `.log` files older than 7 days. It helps keep the project tidy without losing recent logs.

<details>
<summary>Click to see a note on best practices</summary>

`uvx` runs the latest version of a tool in an isolated cache, outside the virtual environment.
This keeps the project light and simple, but behavior can change when the tool updates.
For fully reproducible results, or when you need to use the local `.venv`, use `uv run` instead.

</details>

### 3.3 Run the Data Preparation Script

To execute the data preparation module with relative imports, run the script as part of the package using the -m flag:
python -m src.analytics_project.data_prep

This tells Python to treat the folder as a package, enabling relative imports like:
from .utils_logger import init_logger


Tip: Avoid running the script directly like this:
python src/analytics_project/data_prep.py


Doing so may result in:
ImportError: attempted relative import with no known parent package

Always use -m for package-aware execution.

### 3.4 Build Project Documentation

Make sure you have current doc dependencies, then build your docs, fix any errors, and serve them locally to test.

```shell
uv run mkdocs build --strict
uv run mkdocs serve
```

- After running the serve command, the local URL of the docs will be provided. To open the site, press **CTRL and click** the provided link (at the same time) to view the documentation. On a Mac, use **CMD and click**.
- Press **CTRL c** (at the same time) to stop the hosting process.

### 3.5 Execute

This project includes demo code.
Run the demo Python modules to confirm everything is working.

In VS Code terminal, run:

```shell
uv run python -m analytics_project.demo_module_basics
uv run python -m analytics_project.demo_module_languages
uv run python -m analytics_project.demo_module_stats
uv run python -m analytics_project.demo_module_viz
```

You should see:

- Log messages in the terminal
- Greetings in several languages
- Simple statistics
- A chart window open (close the chart window to continue).

If this works, your project is ready! If not, check:

- Are you in the right folder? (All terminal commands are to be run from the root project folder.)
- Did you run the full `uv sync --extra dev --extra docs --upgrade` command?
- Are there any error messages? (ask for help with the exact error)

---

### 3.6 Git add-commit-push to GitHub

Anytime we make working changes to code is a good time to git add-commit-push to GitHub.

1. Stage your changes with git add.
2. Commit your changes with a useful message in quotes.
3. Push your work to GitHub.

```shell
git add .
git commit -m "describe your change in quotes"
git push -u origin main
```

This will trigger the GitHub Actions workflow and publish your documentation via GitHub Pages.

### 3.7 Modify and Debug

With a working version safe in GitHub, start making changes to the code.

Before starting a new session, remember to do a `git pull` and keep your tools updated.

Each time forward progress is made, remember to git add-commit-push.

1. Environmental Setup:

  If .venv is deleted.
  adding a new package.
  creating a new project.

uv venv
uv pip install -r requirements.txt

2. Running Python:

   uv run python -m analytics_project.<module_name>

3. Running test:

$env:PYTHONPATH = "$PWD/src"
pytest --cov=src --cov-report=term-missing

## 4.1 Data Cleaning with DataScrubber

This project includes a modular data cleaning pipeline using the DataScrubber class, located in
`src/analytics_project/data_preparation/data_scrubber.py`. The DataScrubber provides reusable
cleaning operations that are used by specialized preparation scripts for each data type.

Data Preparation Scripts:
- `prepare_customers_data.py`: Cleans and standardizes customer information
- `prepare_products_data.py`: Processes product catalog data
- `prepare_sales_data.py`: Handles transaction records

Each script can be run independently to process its specific dataset. The scripts:
1. Read raw data from `data/raw/`
2. Apply standardized cleaning steps using DataScrubber:
   - Standardize column names
   - Remove duplicates
   - Handle missing values
   - Apply domain-specific standardization
   - Validate data types and ranges
3. Save cleaned outputs to `data/prepared/`

Schema-Aware Scrubbing
   - Foreign key fields like store_id, campaign_id, and customer_id are validated and coerced to integer types to ensure join safety.
   - campaign_id = 0 is explicitly preserved to represent organic sales (not tied to a campaign).

To run a data preparation script, use the Python module syntax:

```powershell
# From the repository root:
.\.venv\Scripts\python.exe -m analytics_project.data_preparation.prepare_customers_data
.\.venv\Scripts\python.exe -m analytics_project.data_preparation.prepare_products_data
.\.venv\Scripts\python.exe -m analytics_project.data_preparation.prepare_sales_data
```

The cleaned datasets will be saved as:
- `data/prepared/customers_prepared.csv`
- `data/prepared/products_prepared.csv`
- `data/prepared/sales_prepared.csv`

Note: The `DataScrubber` class is a reusable library module that provides the core cleaning
functionality. It is not meant to be run directly but is imported by the preparation scripts.


## 5.1 ETL Design Overview
This project implements a modular ETL pipeline to transform raw retail data into a structured SQLite data warehouse for downstream analytics. The design emphasizes schema integrity, reproducibility, and SQL join practice using mock reference tables.

#### 1. Original Raw Schema
The raw data files contained rich transactional and entity-level information. Below is a summary of the original columns before transformation:

Customers:

Column Name	      Data Type	      Description
customer_id	      INT	            primay key
name	            VARCHAR(100)	  customer name
region	          VARCHAR(50)	    region customer resides
join_date	        DATE	          date customer joined
loyalty_points	  INT	current     loyalty points
engagement_style  VARCHAR(50)	    engagement channel

Products:

Column Name	      Data Type	      Description
product_id	      INT	region c    ustomer resides
product_name	    VARCHAR(100)	  primay key
category	        VARCHAR(50)	    name of product
unit_price	      DECIMAL(10,2)	  price per product
stock_level	      INT	current     inventory unit
supplier_tier	    VARCHAR(50)	    supplier class

Sales:
Column Name	      Data Type	       Description
transaction_id	  INT	unique       ID of transaction
sale_date	        DATE	           date of sale
customer_id	      INT	             foreign key to customers
product_id	      INT	             foreign key to products
store_id	        INT	             foreign key to stores
campaign_id	      INT	             foreign key to campaign
sale_amount	      DECIMAL(10,2)	   amount of sale
discount_percent	DECIMAL(5,2)	   percent of discount
payment_method	  VARCHAR(50)	     method of payment


### 2. ETL Transformations
During the ETL process, several columns were removed or transformed to align with the simplified schema and support SQL join practice:

🔻 Removed Columns
  - name from customer — excluded to focus on regional and behavioral attributes
  - stock_level and supplier_tier from product — excluded to simplify product modeling
  - payment_method from sale — excluded to streamline the sales table for campaign analysis
🔺 Added Mock Tables
    To support SQL join practice and campaign attribution analysis, two mock reference tables were added

These tables were populated at the excution of the ETL process.

Table: customer
  - customer_id
  - region
  - join_date
  - loyalty_points
  - engagement_style

Table: product
  - product_id
  - product_name
  - category
  - unit_price

Table: sale
  - sale_id
  - customer_id
  - product_id
  - store_id
  - campaign_id
  - sale_amount
  - sale_date
  - discount_percent

Table: store
  - store_id
  - store_name
  - region

Table: campaign
  - campaign_id
  - campaign_name
  - start_date
  - end_date

Query example:

  SELECT st.region, ca.campaign_name, SUM(sa.sale_amount)
  FROM sale AS sa
  JOIN store AS st ON sa.store_id = st.store_id
  JOIN campaign AS ca ON sa.campaign_id = ca.campaign_id
  GROUP BY st.region, ca.campaign_name;

  Output:
        region          store_name    total_sales
  0        East     New York Uptown    365219.19
  1       North    Downtown Seattle    354006.48
  2  South-West  Phoenix Outfitters    334159.56
  3        West   Los Angeles Plaza    314082.10

#### 3. ETL Highlights

- Schema Alignment: All foreign key fields were validated and coerced to integer types (Int64) to ensure join safety.
- Date Randomization: sale_date values were randomized across a 6-month range to simulate temporal variation.
- Selective Column Retention: Only analytics-relevant fields were retained to simplify schema and focus on campaign/store joins.


### 4. SQLite Extension Limitation in VS Code
Despite reinstalling the SQLite extension in Visual Studio Code, the expected interface features — such as the "Open Database" option in right-click context menu — did not appear. This prevented direct interaction with .db files through the extension UI.
As a workaround, all SQL operations (including schema creation, data inspection, and joins) were executed using Python scripts via sqlite3 and pandas. This approach ensured full control over database interactions and reproducibility across environments.

 Workaround Strategy
- SQL queries are embedded in Python scripts using cursor.execute() or pd.read_sql_query()
  - SQL Scripts are written in /dw_create/smart_sales_analysis.py.
- Data validation and joins are tested using Python-based queries instead of relying on extension-based exploration
This approach maintains full functionality and avoids reliance on potentially unstable IDE extensions.



