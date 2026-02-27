import pandas as pd
from sqlalchemy import create_engine

engine_vendas = create_engine('postgresql://aluno_readonly:Alunos2026%21@ep-raspy-block-ainzo20a-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require')
engine_estoque = create_engine('postgresql://aluno_readonly:Alunos2026%21@ep-dry-frog-ail6dwj9-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require')
engine_rh = create_engine('postgresql://aluno_readonly:Alunos2026%21@ep-noisy-unit-aiyj66lx-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require')

teste = pd.read_sql('SELECT * FROM produtos', engine_estoque)

print(teste)