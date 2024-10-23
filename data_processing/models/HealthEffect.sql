{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/HealthEffect.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(name AS VARCHAR) AS name,
    CAST(description AS VARCHAR) AS description,
    CAST(chebi_name AS VARCHAR) AS chebi_name,
    TRY_CAST(chebi_id AS INTEGER) AS chebi_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    TRY_CAST(creator_id AS INTEGER) AS creator_id,
    TRY_CAST(updater_id AS INTEGER) AS updater_id,
    CAST(chebi_definition AS VARCHAR) AS chebi_definition

FROM source_data