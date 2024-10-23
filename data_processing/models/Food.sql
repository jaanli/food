{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/Food.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(name AS VARCHAR) AS name,
    CAST(name_scientific AS VARCHAR) AS name_scientific,
    CAST(description AS VARCHAR) AS description,
    CAST(itis_id AS VARCHAR) AS itis_id,
    CAST(wikipedia_id AS VARCHAR) AS wikipedia_id,
    CAST(picture_file_name AS VARCHAR) AS picture_file_name,
    CAST(picture_content_type AS VARCHAR) AS picture_content_type,
    TRY_CAST(picture_file_size AS INTEGER) AS picture_file_size,
    TRY_CAST(picture_updated_at AS TIMESTAMP) AS picture_updated_at,
    TRY_CAST(legacy_id AS INTEGER) AS legacy_id,
    CAST(food_group AS VARCHAR) AS food_group,
    CAST(food_subgroup AS VARCHAR) AS food_subgroup,
    CAST(food_type AS VARCHAR) AS food_type,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    TRY_CAST(creator_id AS INTEGER) AS creator_id,
    TRY_CAST(updater_id AS INTEGER) AS updater_id,
    CAST(export_to_afcdb AS BOOLEAN) AS export_to_afcdb,
    CAST(category AS VARCHAR) AS category,
    TRY_CAST(ncbi_taxonomy_id AS INTEGER) AS ncbi_taxonomy_id,
    CAST(export_to_foodb AS BOOLEAN) AS export_to_foodb,
    CAST(public_id AS VARCHAR) AS public_id

FROM source_data