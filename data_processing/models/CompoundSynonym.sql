{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/CompoundSynonym.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(synonym AS VARCHAR) AS synonym,
    CAST(synonym_source AS VARCHAR) AS synonym_source,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(source_id AS INTEGER) AS source_id,
    CAST(source_type AS VARCHAR) AS source_type

FROM source_data