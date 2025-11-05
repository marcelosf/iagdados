import dagster as dg
from .resources import ComprasGovAPIResource, SqlAlchemyResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "comprasgov_api": ComprasGovAPIResource(
                base_url=dg.EnvVar("COMPRASGOV_API_BASE_URL")
            ),
            "sqlalchemy": SqlAlchemyResource(
                connection_string=dg.EnvVar("MARIA_DB_CONNECTION_STRING")                
            )
        }
    )