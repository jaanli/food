{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/Compound.csv',
                               header=true,
                               filename=true)
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(public_id AS VARCHAR) AS public_id,
    CAST(name AS VARCHAR) AS name,
    CAST(moldb_iupac AS VARCHAR) AS moldb_iupac,
    CAST(state AS VARCHAR) AS state,
    CAST(annotation_quality AS VARCHAR) AS annotation_quality,
    CAST(description AS VARCHAR) AS description,
    CAST(cas_number AS VARCHAR) AS cas_number,
    CAST(moldb_inchikey AS VARCHAR) AS moldb_inchikey,
    CAST(moldb_inchi AS VARCHAR) AS moldb_inchi,
    CAST(moldb_smiles AS VARCHAR) AS moldb_smiles,
    TRY_CAST(moldb_mono_mass AS DECIMAL(18,9)) AS moldb_mono_mass,
    CAST(kingdom AS VARCHAR) AS kingdom,
    CAST(superklass AS VARCHAR) AS superklass,
    CAST(klass AS VARCHAR) AS klass,
    CAST(subklass AS VARCHAR) AS subklass

FROM source_data