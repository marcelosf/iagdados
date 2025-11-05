import dagster as dg
from sqlalchemy import create_engine
from requests_tor import RequestsTor

class ComprasGovAPIResource(dg.ConfigurableResource):
    base_url: str
    stauts_item: str = "true"

    def get_items(self, page: int, page_width: int, group_code: int):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
        }
        parameters = {
            "pagina": page,
            "tamanhoPagina": page_width,
            "codigoGrupo": group_code,
            "statusitem": self.stauts_item
        }

        tor = RequestsTor(tor_ports=(9050,), tor_cport=9051)

        response = tor.get(self.base_url, params=parameters, headers=headers)
        return response


class SqlAlchemyResource(dg.ConfigurableResource):
    connection_string: str
    
    def get_engine(self):
        engine = create_engine(self.connection_string)
        return engine


