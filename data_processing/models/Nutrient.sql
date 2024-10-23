{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/Nutrient.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    TRY_CAST(legacy_id AS INTEGER) AS legacy_id,
    CAST(type AS VARCHAR) AS type,
    CAST(public_id AS VARCHAR) AS public_id,
    CAST(name AS VARCHAR) AS name,
    CAST(export AS BOOLEAN) AS export,
    CAST(state AS VARCHAR) AS state,
    CAST(annotation_quality AS VARCHAR) AS annotation_quality,
    CAST(description AS VARCHAR) AS description,
    CAST(wikipedia_id AS VARCHAR) AS wikipedia_id,
    CAST(comments AS VARCHAR) AS comments,
    CAST(dfc_id AS VARCHAR) AS dfc_id,
    TRY_CAST(duke_id AS INTEGER) AS duke_id,
    TRY_CAST(eafus_id AS INTEGER) AS eafus_id,
    CAST(dfc_name AS VARCHAR) AS dfc_name,
    CAST(compound_source AS VARCHAR) AS compound_source,
    CAST(metabolism AS VARCHAR) AS metabolism,
    CAST(synthesis_citations AS VARCHAR) AS synthesis_citations,
    CAST(general_citations AS VARCHAR) AS general_citations,
    TRY_CAST(creator_id AS INTEGER) AS creator_id,
    TRY_CAST(updater_id AS INTEGER) AS updater_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at

FROM source_data