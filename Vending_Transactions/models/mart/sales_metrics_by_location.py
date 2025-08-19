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