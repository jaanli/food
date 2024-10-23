{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/CompoundOntologyTerm.csv',
                               header=true,
                               filename=true)
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(compound_id AS INTEGER) AS compound_id,
    CAST(export AS BOOLEAN) AS export,
    CAST(ontology_term_id AS INTEGER) AS ontology_term_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at

FROM source_data