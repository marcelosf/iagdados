import pandas as pd
import dagster as dg
from pathlib import Path
from .resources import ComprasGovAPIResource, SqlAlchemyResource, ComprasgovTableResource
from sqlalchemy.orm import Session


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


@dg.asset(kinds={"pandas"})
def items_keys_mapping(
    context: dg.AssetExecutionContext,
    raw_items_parquet
):
    items_df = pd.read_parquet(raw_items_parquet)
    keys_mapping = {
        "codigoItem": "codigo_item",
        "codigoGrupo": "codigo_grupo",
        "nomeGrupo": "nome_grupo",
        "codigoClasse": "codigo_classe",
        "nomeClasse": "nome_classe",
        "codigoPdm": "codigo_pdm",
        "nomePdm": "nome_pdm",
        "descricaoItem": "descricao_item",
        "statusItem": "status_item",
        "itemSustentavel": "item_sustentavel",
        "codigoNcm": "codigo_ncm",
        "descricaoNcm": "descricao_ncm",
        "dataHoraAtualizacao": "data_hora_atualizacao"
    }
    renamed_df = items_df.rename(columns=keys_mapping)
    return renamed_df


@dg.asset(kinds={"parquet"})
def silver_items_parquet(
    context: dg.AssetExecutionContext,
    items_keys_mapping: pd.DataFrame
):
    filename = "silver_items"
    file_path = (Path(__file__).parent / f"../../data/silver/{filename}.parquet")
    items_keys_mapping.to_parquet(file_path)
    return file_path


@dg.asset(kinds={"sqlalchemy", "mariadb", "pandas"})
def items_data_loading(
    sqlalchemy: SqlAlchemyResource,
    items_keys_mapping: pd.DataFrame,
    comprasgov_table: ComprasgovTableResource
):
    engine = sqlalchemy.get_engine()
    data = items_keys_mapping.to_dict(orient="records")
    ComprasGovTable = comprasgov_table.create_comprasgov_itens_table(engine=engine)

    with Session(engine) as session:
        session.bulk_insert_mappings(ComprasGovTable, data)
        session.commit()
