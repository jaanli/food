{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/CompoundsHealthEffect.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(compound_id AS INTEGER) AS compound_id,
    CAST(health_effect_id AS INTEGER) AS health_effect_id,
    CAST(orig_health_effect_name AS VARCHAR) AS orig_health_effect_name,
    CAST(orig_compound_name AS VARCHAR) AS orig_compound_name,
    CAST(orig_citation AS VARCHAR) AS orig_citation,
    CAST(citation AS VARCHAR) AS citation,
    CAST(citation_type AS VARCHAR) AS citation_type,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(creator_id AS INTEGER) AS creator_id,
    CAST(updater_id AS INTEGER) AS updater_id,
    CAST(source_id AS INTEGER) AS source_id,
    CAST(source_type AS VARCHAR) AS source_type

FROM source_data