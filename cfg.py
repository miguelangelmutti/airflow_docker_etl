from decouple import AutoConfig
from constants import ROOT_DIR

config = AutoConfig(search_path=ROOT_DIR)

DB_CONNSTR = config ("DATABASE_URL")

MUSEO_URL = config ("MUSEO_URL" )
CINES_URL = config ("CINES_URL")
ESPACIOS_URL = config ("ESPACIOS_URL" )

museo_ds = {
"name": "museos",
"url": MUSEO_URL,
}

cines_ds = {
"name": "cines",
"url": CINES_URL,
}

espacios_ds = {
"name": "bibliotecas",
"url": ESPACIOS_URL,
}

categorias = [
    museo_ds,
    cines_ds,
    espacios_ds
]