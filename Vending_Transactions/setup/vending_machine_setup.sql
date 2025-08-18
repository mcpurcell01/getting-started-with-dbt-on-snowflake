USE ROLE accountadmin;

-- Set a variable to store the current user's name
SET CURRENT_USER_NAME = (SELECT CURRENT_USER());

-- Use the variable to create a unique warehouse for the current user.
CREATE OR REPLACE WAREHOUSE IDENTIFIER($CURRENT_USER_NAME || '_vending_machine_dbt_wh')
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for vending machine dbt demo for ' || $CURRENT_USER_NAME;

-- Set the context to use the unique warehouse
USE WAREHOUSE IDENTIFIER($CURRENT_USER_NAME || '_vending_machine_dbt_wh');

-- Use the variable to create a unique database for the current user.
CREATE DATABASE IF NOT EXISTS IDENTIFIER($CURRENT_USER_NAME || '_vending_machine_dbt_db');

-- All subsequent schemas and tables will be created within this unique database,
-- so you don't need to add the user name to their names.
USE DATABASE IDENTIFIER($CURRENT_USER_NAME || '_vending_machine_dbt_db');

-- Create the schemas
CREATE OR REPLACE SCHEMA raw;
CREATE OR REPLACE SCHEMA dev;
CREATE OR REPLACE SCHEMA prod;

-- Set up logging and tracing for schemas
ALTER SCHEMA dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA dev SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA prod SET LOG_LEVEL = 'INFO';
ALTER SCHEMA prod SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA prod SET METRIC_LEVEL = 'ALL';

-- Create the API integrations and network rules
-- Fully qualifying these objects for clarity as they are created in the public schema
CREATE OR REPLACE API INTEGRATION git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ENABLED = TRUE;

CREATE OR REPLACE NETWORK RULE public.dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_access_integration
  ALLOWED_NETWORK_RULES = (dbt_network_rule)
  ENABLED = true;

-- Create a file format and external stage for loading data
CREATE OR REPLACE FILE FORMAT public.csv_ff
skip_header=1
type = 'csv';

CREATE OR REPLACE STAGE public.s3load
COMMENT = 'Vending Machine S3 Stage Connection'
file_format = public.csv_ff;


/*--
 raw zone table build
--*/

-- Create the VENDING_TRANSACTIONS table in the raw schema
CREATE OR REPLACE TABLE raw.vending_transactions
(
    transaction_id VARCHAR(16777216),
    vending_machine_id VARCHAR(16777216),
    transaction_timestamp TIMESTAMP_NTZ(9),
    product_id VARCHAR(16777216),
    quantity NUMBER(5,0),
    transaction_total_amount NUMBER(38,4),
    payment_method VARCHAR(16777216),
    customer_id VARCHAR(16777216)
)
COMMENT = 'Raw data for vending machine transactions.';

-- Create the PRODUCT_DETAILS table in the raw schema
CREATE OR REPLACE TABLE raw.product_details
(
    product_id VARCHAR(16777216),
    product_name VARCHAR(16777216),
    product_category VARCHAR(16777216),
    unit_price NUMBER(38,4)
)
COMMENT = 'Raw data for product details.';

-- Create the SURVEY_FEEDBACK table in the raw schema
CREATE OR REPLACE TABLE raw.survey_feedback
(
    survey_id VARCHAR(16777216),
    transaction_id VARCHAR(16777216),
    customer_id VARCHAR(16777216),
    satisfaction_score NUMBER(5,0)
)
COMMENT = 'Raw data from customer satisfaction surveys.';

-- Create the VENDING_MACHINES table in the raw schema
CREATE OR REPLACE TABLE raw.vending_machines
(
    vending_machine_id VARCHAR(16777216),
    location_city VARCHAR(16777216),
    region VARCHAR(16777216),
    country VARCHAR(16777216)
)
COMMENT = 'Raw data for vending machines.';

-- Create the CUSTOMER_DETAILS table in the raw schema
CREATE OR REPLACE TABLE raw.customer_details
(
    customer_id VARCHAR(16777216),
    age NUMBER(3,0),
    gender VARCHAR(16777216),
    email_address VARCHAR(16777216)
)
COMMENT = 'Raw data for customer details.';


/*--
 raw zone table load
--*/

-- Note: The COPY INTO statements below use a placeholder S3 URL.
-- You will need to replace the URL with the actual location of your data files.

-- Load the VENDING_TRANSACTIONS table
COPY INTO raw.vending_transactions
FROM @public.s3load/vending_transactions.csv;

-- Load the PRODUCT_DETAILS table
COPY INTO raw.product_details
FROM @public.s3load/product_details.csv;

-- Load the SURVEY_FEEDBACK table
COPY INTO raw.survey_feedback
FROM @public.s3load/survey_feedback.csv;

-- Load the VENDING_MACHINES table
COPY INTO raw.vending_machines
FROM @public.s3load/vending_machines.csv;

-- Load the CUSTOMER_DETAILS table
COPY INTO raw.customer_details
FROM @public.s3load/customer_details.csv;

-- setup completion note
SELECT 'vending_machine_dbt_db setup is now complete' AS note;
