{% snapshot sales_metrics_snapshot %}

{{
    config(
      target_schema='dev',
      unique_key='LOCATION_CITY',
      strategy='check',
      check_cols=['TOTAL_SALES', 'TRANSACTION_COUNT']
    )
}}

select * from {{ ref('sales_metrics_by_location') }}

{% endsnapshot %}