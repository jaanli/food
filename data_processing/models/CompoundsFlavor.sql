{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/CompoundsFlavor.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(compound_id AS INTEGER) AS compound_id,
    CAST(flavor_id AS INTEGER) AS flavor_id,
    CAST(citations AS VARCHAR) AS citations,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(creator_id AS INTEGER) AS creator_id,
    CAST(updater_id AS INTEGER) AS updater_id,
    CAST(source_id AS INTEGER) AS source_id,
    CAST(source_type AS VARCHAR) AS source_type

FROM source_data