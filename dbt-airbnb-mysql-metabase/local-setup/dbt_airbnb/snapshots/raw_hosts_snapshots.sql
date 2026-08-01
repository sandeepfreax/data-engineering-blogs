{% snapshot scd_raw_hosts %}

{{
    config(
      target_schema='airbnb',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      hard_deletes='invalidate'
    )
}}

select * from {{ source('airbnb', 'hosts') }}

{% endsnapshot %}