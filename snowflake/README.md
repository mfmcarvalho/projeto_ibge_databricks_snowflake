# Snowflake

Store the warehouse setup and DDL scripts in this directory.

Recommended file:

```text
setup_pix_db.sql
```

Database:

```text
PIX_DB
```

Schemas:

```text
BRONZE
SILVER
GOLD
```

The current loader also creates and populates the normalized VAB structures used by Power BI.

Before publishing, ensure the setup SQL is aligned with the latest loader so that `DIM_CATEGORIA_VAB` and `FACT_VAB_MUNICIPIO` are represented in the repository DDL.
