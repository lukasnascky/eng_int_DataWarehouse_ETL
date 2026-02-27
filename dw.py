import pandas as pd
from sqlalchemy import create_engine

engine_vendas = create_engine('postgresql://aluno_readonly:Alunos2026%21@ep-raspy-block-ainzo20a-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require')
engine_estoque = create_engine('postgresql://aluno_readonly:Alunos2026%21@ep-dry-frog-ail6dwj9-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require')
engine_rh = create_engine('postgresql://aluno_readonly:Alunos2026%21@ep-noisy-unit-aiyj66lx-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require')

engine_dw = create_engine('postgresql+psycopg2://neondb_owner:npg_54WbqlwyeRLg@ep-odd-surf-acyja679-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')


# TABELA FILIAIS
df_filiais_vendas = pd.read_sql('SELECT * FROM filiais', engine_vendas)
df_filiais_estoque = pd.read_sql('SELECT * FROM filiais_estoque', engine_estoque)
df_filiais_rh = pd.read_sql('SELECT * FROM filiais_rh', engine_rh)

de_para_filiais = pd.DataFrame({
    'id_filial': [1, 2, 3, 4, 5, 6, 7],
    'sigla_estoque': ['FL-SP-01', 'FL-SP-02', 'FL-RJ-01', 'FL-RJ-02', 'FL-MG-01', 'FL-SP-03', 'FL-DF-01'] 
})

filiais = pd.merge(de_para_filiais, df_filiais_vendas, left_on='id_filial', right_on='codigo_filial')
filiais = pd.merge(filiais, df_filiais_estoque, left_on='sigla_estoque', right_on='sigla_filial')
filiais = pd.merge(filiais, df_filiais_rh, left_on='id_filial', right_on='id_filial_rh')

filiais = filiais[[
    'id_filial', 'sigla_filial', 'nome_filial', 'razao_social', 'cnpj', 'endereco', 'regiao', 'cidade', 'uf', 'cep', 'telefone', 'gerente_responsavel', 'status'
]]

filiais['nome_filial'] = filiais['nome_filial'].str.upper()
filiais['razao_social'] = filiais['razao_social'].str.upper()
filiais['endereco'] = filiais['endereco'].str.upper().str.rsplit(', ', n=1).str[0]
filiais['regiao'] = filiais['regiao'].str.upper()
filiais['cidade'] = filiais['cidade'].str.upper()
filiais['regiao'] = filiais['regiao'].str.upper()
filiais['gerente_responsavel'] = filiais['gerente_responsavel'].str.upper()
filiais['status'] = filiais['status'].map({1: True, 0: False})

#filiais.to_csv('filiais.csv')


# TABELA VENDAS
df_vendas = pd.read_sql('SELECT * FROM vendas', engine_vendas)
df_formas_pagamento = pd.read_sql('SELECT * FROM formas_pagamento', engine_vendas)

vendas = pd.merge(df_vendas, df_formas_pagamento, left_on='id_forma_pgto', right_on='id_forma')

vendas['data_venda'] = pd.to_datetime(vendas['data_venda'], format='%d/%m/%Y')
vendas['valor_total_centavos'] = vendas['valor_total_centavos'] / 100
vendas['desconto_centavos'] = vendas['desconto_centavos'] / 100
vendas['descricao'] = vendas['descricao'].str.upper()
vendas['status'] = vendas['status'].map({'A': True, 'C': False})

vendas = vendas[[
    'id_venda', 'codigo_filial', 'id_vendedor', 'cpf_cliente', 'descricao', 'data_venda', 'hora_venda',
    'valor_total_centavos', 'desconto_centavos', 'status'
]]

vendas = vendas.rename(columns={
    'codigo_filial': 'id_filial',
    'descricao': 'forma_pagamento',
    'valor_total_centavos': 'valor_total',
    'desconto_centavos': 'desconto'
})

#vendas.to_csv('vendas.csv')

# TABELA ITENS VENDA