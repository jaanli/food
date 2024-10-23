{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/OntologyTerm.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(term AS VARCHAR) AS term,
    CAST(definition AS VARCHAR) AS definition,
    CAST(external_id AS VARCHAR) AS external_id,
    CAST(external_source AS VARCHAR) AS external_source,
    CAST(comment AS VARCHAR) AS comment,
    CAST(curator AS VARCHAR) AS curator,
    TRY_CAST(parent_id AS INTEGER) AS parent_id,
    CAST(level AS INTEGER) AS level,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    TRY_CAST(legacy_id AS INTEGER) AS legacy_id

FROM source_data