import dagster as dg
import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData, Column, Integer, String
from .resources import ComprasGovAPIResource, SqlAlchemyResource


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
    engine = sqlalchemy.get_engine()

    metadata = MetaData()
    Table(
        "comprasgov_items",
        metadata,
        Column("codigo_item", Integer, primary_key=True),
        Column("codigo_grupo", Integer),
        Column("nome_grupo", String(255)),
        Column("codigo_classe", Integer),
        Column("nome_classe", String(2048)),
        Column("codigo_pdm", Integer),
        Column("nome_pdm", String(2048)),
        Column("nome_item", String(2048)),
        Column("codigo_ncm", Integer),
        Column("descricriao_ncm", String(2048))
    )
    metadata.create_all(engine)


@dg.asset(kinds={"pandas"})
def items_data_mapping(
    context: dg.AssetExecutionContext,
    raw_items_parquet
):
    df = pd.read_parquet(raw_items_parquet)
    keys_mapping = {
        "codigoItem": "codigo_item",
        "codigoGrupo": "codigo_grupo",
        "nomeGrupo": "nome_grupo",
        "codigoClasse": "codigo_classe",
        "nomeClasse": "nome_classe",
        "codigoPdm": "codigo_pdm",
        "nomePdm": "nome_pdm",
        "nomeItem": "nome_item",
        "codigoNcm": "codigo_ncm",
        "descricaoNcm": "descricao_ncm"
    }    
