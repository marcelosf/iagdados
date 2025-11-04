import dagster as dg


from . import assets


comprasgov_job = dg.define_asset_job(
    name="comprasgov_job",
    selection=[
        assets.raw_item_dataframe,
        assets.raw_items_parquet,
        assets.mariadb_items_table,
    ]
)