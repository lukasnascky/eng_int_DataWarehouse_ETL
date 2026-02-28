import pandas as pd
from sqlalchemy import create_engine

engine_dw = create_engine('postgresql+psycopg2://neondb_owner:npg_sd2BoQH5AVja@ep-tiny-cell-acd1wnhz-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')


teste = pd.read_sql('SELECT * FROM filiais', engine_dw)

print(teste)