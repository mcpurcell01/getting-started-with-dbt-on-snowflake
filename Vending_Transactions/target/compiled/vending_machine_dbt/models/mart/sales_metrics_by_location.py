from snowflake.snowpark.functions import col, lit, concat, count, sum as sum_, coalesce

def model(dbt, session):
    """
    This Python model demonstrates Snowpark transformations to calculate sales metrics
    by vending machine. It joins transaction data with machine and product details.
    """
    # Get tables using dbt's ref function to reference the raw models
    machines_df = dbt.ref('raw_vending_machines')
    transactions_df = dbt.ref('raw_vending_transactions')
    
    # Aggregate transactions to get total sales and transactions per machine
    machine_metrics = (
        transactions_df
        .join(machines_df, "VENDING_MACHINE_ID", "inner")
        .groupBy("VENDING_MACHINE_ID", "LOCATION_CITY", "REGION", "COUNTRY")
        .agg(
            sum_("TRANSACTION_TOTAL_AMOUNT").alias("TOTAL_SALES"),
            count("TRANSACTION_ID").alias("TRANSACTION_COUNT")
        )
    )
    
    # Return the final dataframe with key metrics
    return machine_metrics


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"raw_vending_machines": "INSTRUCTOR2_vending_machine_dbt_db.dev.raw_vending_machines", "raw_vending_transactions": "INSTRUCTOR2_vending_machine_dbt_db.dev.raw_vending_transactions"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "INSTRUCTOR2_vending_machine_dbt_db"
    schema = "dev"
    identifier = "sales_metrics_by_location"
    
    def __repr__(self):
        return 'INSTRUCTOR2_vending_machine_dbt_db.dev.sales_metrics_by_location'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


