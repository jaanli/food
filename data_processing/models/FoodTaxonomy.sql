{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/FoodTaxonomy.csv',
                               header=true,
                               filename=true,
                               quote='"')
),

cleaned_data AS (
    SELECT
        *,
        REGEXP_REPLACE(classification_name, '^"|"$', '') AS clean_classification_name
    FROM source_data
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(food_id AS INTEGER) AS food_id,
    CAST(ncbi_taxonomy_id AS INTEGER) AS ncbi_taxonomy_id,
    CAST(clean_classification_name AS VARCHAR) AS classification_name,
    CAST(classification_order AS INTEGER) AS classification_order,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at

FROM cleaned_data