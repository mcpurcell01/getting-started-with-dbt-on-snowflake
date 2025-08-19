



select
    1
from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS

where not(TRANSACTION_TIMESTAMP  <= current_timestamp())

