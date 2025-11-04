import dagster as dg
import pandas as pd
from pathlib import Path
from dagster_sqlalchemy import SqlAlchemyResource
from sqlalchemy import Table, MetaData, Column, Integer, String
from .resources import ComprasGovAPIResource


@dg.asset(kinds={"pandas"})
def raw_item_dataframe(
    context: dg.AssetExecutionContext,
    comprasgov_api: ComprasGovAPIResource
) -> pd.DataFrame:
    items = comprasgov_api.get_items(page=1, page_width=500, group_code=70)
    context.log.info(f"Status Code: {items.status_code}")
    df = pd.DataFrame(items.json()["resultado"])
    return df


@dg.asset(kinds={"parquet"})
def raw_items_parquet(
    context: dg.AssetExecutionContext,
    raw_item_dataframe: pd.DataFrame
):
    filename = "raw_items"
    file_path = (Path(__file__).parent / f"../../data/raw/{filename}.parquet")
    raw_item_dataframe.to_parquet(file_path)
    return file_path


@dg.asset(kinds={"sqlalchemy", "mariadb"})
def mariadb_items_table(context: dg.AssetExecutionContext, sqlalchemy: SqlAlchemyResource):
    engine = sqlalchemy.engine

    metadata = MetaData()
    Table(
        "comprasgov_items",
        metadata,
        Column("codigo_grupo", Integer),
        Column("nome_grupo", String(255)),
        Column("codigo_classe", Integer),
        Column("nome_classe", String(2048)),
        Column("codigo_pdm", Integer),
        Column("nome_pdm", String(2048)),
        Column("codigo_item", Integer, primary_key=True),
        Column("nome_item", String(2048)),
        Column("codigo_ncm", Integer),
        Column("descricriao_ncm", String(2048))
    )
    metadata.create_all(engine)
    return engine

