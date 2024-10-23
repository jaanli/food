{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/CompoundsEnzyme.csv',
                               header=true,
                               filename=true)
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(compound_id AS INTEGER) AS compound_id,
    CAST(enzyme_id AS INTEGER) AS enzyme_id,
    CAST(citations AS VARCHAR) AS citations,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(creator_id AS INTEGER) AS creator_id,
    CAST(updater_id AS INTEGER) AS updater_id

FROM source_data