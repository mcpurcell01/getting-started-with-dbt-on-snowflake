USE ROLE accountadmin;

-- Create the warehouse for your dbt project
CREATE OR REPLACE WAREHOUSE vending_machine_dbt_wh
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for vending machine dbt demo';

USE WAREHOUSE vending_dbt_wh;

-- Create the database and schemas
CREATE DATABASE IF NOT EXISTS vending_machine_dbt_db;
CREATE OR REPLACE SCHEMA vending_machine_dbt_db.raw;
CREATE OR REPLACE SCHEMA vending_machine_dbt_db.dev;
CREATE OR REPLACE SCHEMA vending_machine_dbt_db.prod;

-- Set up logging and tracing for schemas
ALTER SCHEMA vending_machine_dbt_db.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA vending_machine_dbt_db.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA vending_machine_dbt_db.dev SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA vending_machine_dbt_db.prod SET LOG_LEVEL = 'INFO';
ALTER SCHEMA vending_machine_dbt_db.prod SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA vending_machine_dbt_db.prod SET METRIC_LEVEL = 'ALL';

-- Create the API integrations and network rules for dbt to function
CREATE OR REPLACE API INTEGRATION git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ENABLED = TRUE;

CREATE OR REPLACE NETWORK RULE vending_dbt_db.public.dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_access_integration
  ALLOWED_NETWORK_RULES = (vending_dbt_db.public.dbt_network_rule)
  ENABLED = true;

-- Create a file format and external stage for loading data
CREATE OR REPLACE FILE FORMAT vending_machine_dbt_db.public.csv_ff
skip_header=1
type = 'csv';

CREATE OR REPLACE STAGE vending_machine_dbt_db.public.s3load
COMMENT = 'Vending Machine S3 Stage Connection'
-- url = 's3://sfquickstarts/vending_machine/' -- This URL is a placeholder
file_format = vending_dbt_db.public.csv_ff;


/*--
 raw zone table build
--*/

-- Create the VENDING_TRANSACTIONS table
CREATE OR REPLACE TABLE vending_machine_dbt_db.raw.vending_transactions
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

-- Create the PRODUCT_DETAILS table
CREATE OR REPLACE TABLE vending_machine_dbt_db.raw.product_details
(
    product_id VARCHAR(16777216),
    product_name VARCHAR(16777216),
    product_category VARCHAR(16777216),
    unit_price NUMBER(38,4)
)
COMMENT = 'Raw data for product details.';

-- Create the SURVEY_FEEDBACK table
CREATE OR REPLACE TABLE vending_machine_dbt_db.raw.survey_feedback
(
    survey_id VARCHAR(16777216),
    transaction_id VARCHAR(16777216),
    customer_id VARCHAR(16777216),
    satisfaction_score NUMBER(5,0)
)
COMMENT = 'Raw data from customer satisfaction surveys.';

-- Create the VENDING_MACHINES table
CREATE OR REPLACE TABLE vending_machine_dbt_db.raw.vending_machines
(
    vending_machine_id VARCHAR(16777216),
    location_city VARCHAR(16777216),
    region VARCHAR(16777216),
    country VARCHAR(16777216)
)
COMMENT = 'Raw data for vending machines.';

-- Create the CUSTOMER_DETAILS table
CREATE OR REPLACE TABLE vending_machine_dbt_db.raw.customer_details
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
COPY INTO vending_machine_dbt_db.raw.vending_transactions
--FROM @vending_dbt_db.public.s3load/vending_transactions/;
FROM @vending_machine_dbt_db.public.s3load/vending_transactions.csv;

-- Load the PRODUCT_DETAILS table
COPY INTO vending_machine_dbt_db.raw.product_details
--FROM @vending_dbt_db.public.s3load/product_details/;
FROM @vending_machine_dbt_db.public.s3load/product_details.csv;

-- Load the SURVEY_FEEDBACK table
COPY INTO vending_machine_dbt_db.raw.survey_feedback
--FROM @vending_dbt_db.public.s3load/survey_feedback/;
FROM @vending_machine_dbt_db.public.s3load/survey_feedback.csv;

-- Load the VENDING_MACHINES table
COPY INTO vending_machine_dbt_db.raw.vending_machines
-- FROM @vending_dbt_db.public.s3load/vending_machines/;
FROM @vending_machine_dbt_db.public.s3load/vending_machines.csv;

-- Load the CUSTOMER_DETAILS table
COPY INTO vending_machine_dbt_db.raw.customer_details
--FROM @vending_dbt_db.public.s3load/customer_details/;
FROM @vending_machine_dbt_db.public.s3load/customer_details.csv;

-- setup completion note
SELECT 'vending_machine_dbt_db setup is now complete' AS note;
