{{ config(
    materialized = 'external',
    location = '~/data/foodb/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/foodb/Enzyme.csv',
                               header=true,
                               filename=true,
                               quote='"')
)

SELECT
    CAST(id AS INTEGER) AS id,
    CAST(name AS VARCHAR) AS name,
    CAST(gene_name AS VARCHAR) AS gene_name,
    CAST(description AS VARCHAR) AS description,
    CAST(go_classification AS VARCHAR) AS go_classification,
    CAST(general_function AS VARCHAR) AS general_function,
    CAST(specific_function AS VARCHAR) AS specific_function,
    CAST(pathway AS VARCHAR) AS pathway,
    CAST(reaction AS VARCHAR) AS reaction,
    CAST(cellular_location AS VARCHAR) AS cellular_location,
    CAST(signals AS VARCHAR) AS signals,
    CAST(transmembrane_regions AS VARCHAR) AS transmembrane_regions,
    CAST(molecular_weight AS VARCHAR) AS molecular_weight,
    CAST(theoretical_pi AS VARCHAR) AS theoretical_pi,
    CAST(locus AS VARCHAR) AS locus,
    CAST(chromosome AS VARCHAR) AS chromosome,
    CAST(uniprot_name AS VARCHAR) AS uniprot_name,
    CAST(uniprot_id AS VARCHAR) AS uniprot_id,
    CAST(pdb_id AS VARCHAR) AS pdb_id,
    CAST(genbank_protein_id AS VARCHAR) AS genbank_protein_id,
    CAST(genbank_gene_id AS VARCHAR) AS genbank_gene_id,
    CAST(genecard_id AS VARCHAR) AS genecard_id,
    CAST(genatlas_id AS VARCHAR) AS genatlas_id,
    CAST(hgnc_id AS VARCHAR) AS hgnc_id,
    CAST(hprd_id AS VARCHAR) AS hprd_id,
    CAST(organism AS VARCHAR) AS organism,
    CAST(general_citations AS VARCHAR) AS general_citations,
    CAST(comments AS VARCHAR) AS comments,
    CAST(creator_id AS INTEGER) AS creator_id,
    CAST(updater_id AS INTEGER) AS updater_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at

FROM source_data