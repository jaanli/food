{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/OntologySynonym.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(ontology_term_id AS INTEGER) AS ontology_term_id,
    CAST(synonym AS VARCHAR) AS synonym,
    CAST(external_id AS VARCHAR) AS external_id,
    CAST("external_srouce" AS VARCHAR) AS external_source,  -- Correcting the typo in column name
    TRY_CAST(parent_id AS INTEGER) AS parent_id,
    CAST(parent_source AS VARCHAR) AS parent_source,
    CAST(comment AS VARCHAR) AS comment,
    CAST(curator AS VARCHAR) AS curator,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at

FROM source_data