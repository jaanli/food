{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/AccessionNumber.csv',
                               header=true,
                               filename=true)
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(number AS VARCHAR) AS number,
    CAST(compound_id AS INTEGER) AS compound_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(source_id AS INTEGER) AS source_id,
    CAST(source_type AS VARCHAR) AS source_type

FROM source_data