{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/Content.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(source_id AS INTEGER) AS source_id,
    CAST(source_type AS VARCHAR) AS source_type,
    CAST(food_id AS INTEGER) AS food_id,
    CAST(orig_food_id AS VARCHAR) AS orig_food_id,
    CAST(orig_food_common_name AS VARCHAR) AS orig_food_common_name,
    CAST(orig_food_scientific_name AS VARCHAR) AS orig_food_scientific_name,
    CAST(orig_food_part AS VARCHAR) AS orig_food_part,
    CAST(orig_source_id AS VARCHAR) AS orig_source_id,
    CAST(orig_source_name AS VARCHAR) AS orig_source_name,
    TRY_CAST(orig_content AS DECIMAL(18,2)) AS orig_content,
    TRY_CAST(orig_min AS DECIMAL(18,2)) AS orig_min,
    TRY_CAST(orig_max AS DECIMAL(18,2)) AS orig_max,
    CAST(orig_unit AS VARCHAR) AS orig_unit,
    CAST(orig_citation AS VARCHAR) AS orig_citation,
    CAST(citation AS VARCHAR) AS citation,
    CAST(citation_type AS VARCHAR) AS citation_type,
    CAST(creator_id AS INTEGER) AS creator_id,
    CAST(updater_id AS INTEGER) AS updater_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(orig_method AS VARCHAR) AS orig_method,
    CAST(orig_unit_expression AS VARCHAR) AS orig_unit_expression,
    TRY_CAST(standard_content AS DECIMAL(18,2)) AS standard_content,
    CAST(preparation_type AS VARCHAR) AS preparation_type,
    CAST(export AS BOOLEAN) AS export

FROM source_data