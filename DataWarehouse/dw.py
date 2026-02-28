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
    'id_filial', 'nome_filial', 'razao_social', 'cnpj', 'endereco', 'cidade', 'uf', 'regiao', 'cep', 'telefone', 'gerente_responsavel', 'status'
]]

filiais['nome_filial'] = filiais['nome_filial'].str.upper()
filiais['razao_social'] = filiais['razao_social'].str.upper()
filiais['endereco'] = filiais['endereco'].str.upper().str.rsplit(', ', n=1).str[0]
filiais['regiao'] = filiais['regiao'].str.upper()
filiais['cidade'] = filiais['cidade'].str.upper()
filiais['regiao'] = filiais['regiao'].str.upper()
filiais['gerente_responsavel'] = filiais['gerente_responsavel'].str.upper()
filiais['status'] = filiais['status'].map({1: True, 0: False})

#filiais.to_csv('Tables/filiais.csv')


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

#vendas.to_csv('Tables/vendas.csv')


# TABELA ITENS VENDA
df_itens_venda = pd.read_sql('SELECT * FROM itens_venda', engine_vendas)
df_produtos = pd.read_sql('SELECT id_produto, descricao FROM produtos', engine_estoque)

itens_venda = pd.merge(df_itens_venda, df_produtos, left_on='cod_produto', right_on='id_produto')

itens_venda['preco_unitario_centavos'] = itens_venda['preco_unitario_centavos'] / 100
itens_venda['subtotal_centavos'] = itens_venda['subtotal_centavos'] / 100

itens_venda = itens_venda[[
    'id_item', 'id_venda', 'cod_produto', 'descricao', 'quantidade', 'preco_unitario_centavos',
    'subtotal_centavos'
]]

itens_venda = itens_venda.rename(columns={
    'cod_produto': 'id_produto',
    'descricao': 'nome_produto',
    'preco_unitario_centavos': 'preco_unitario',
    'subtotal_centavos': 'subtotal'
})

#itens_venda.to_csv('Tables/itens_venda.csv')


# TABELA PRODUTOS
df_produtos = pd.read_sql('SELECT * FROM produtos', engine_estoque)
df_categorias = pd.read_sql('SELECT id_categoria, nome_categoria FROM categorias', engine_estoque)

produtos = pd.merge(df_produtos, df_categorias, on='id_categoria')

produtos['nome_categoria'] = produtos['nome_categoria'].str.upper()
produtos['ativo'] = produtos['ativo'].map({'ativo': True, 'inativo': False})

produtos = produtos[[
    'id_produto', 'ean', 'descricao', 'nome_categoria', 'unidade', 'preco_custo', 'preco_venda',
    'margem_lucro', 'peso_gramas', 'controlado', 'ativo', 'data_cadastro'
]]

produtos = produtos.rename(columns={
    'descricao': 'nome_produto',
    'nome_categoria': 'categoria',
    'ativo': 'status'
})

#produtos.to_csv('Tables/produtos.csv')


# TABELA ENTRADA MERCADORIAS
df_entrada = pd.read_sql('SELECT * FROM entradas_mercadoria', engine_estoque)
de_para_filiais = pd.DataFrame({
    'id_filial': [1, 2, 3, 4, 5, 6, 7],
    'sigla_estoque': ['FL-SP-01', 'FL-SP-02', 'FL-RJ-01', 'FL-RJ-02', 'FL-MG-01', 'FL-SP-03', 'FL-DF-01'] 
})

entradas_mercadoria = pd.merge(de_para_filiais, df_entrada, left_on='sigla_estoque' , right_on='sigla_filial')

entradas_mercadoria['status'] = entradas_mercadoria['status'].map({'ativo': True, 'inativo': False})

entradas_mercadoria = entradas_mercadoria[[
    'id_entrada', 'id_filial', 'id_fornecedor', 'numero_nf', 'data_entrada', 'valor_total', 'status'
]]

#entradas_mercadoria.to_csv('Tables/entradas_mercadorias.csv')


#TABELA ESTOQUE
estoque = pd.read_sql('SELECT * FROM estoque', engine_estoque)

estoque = pd.merge(de_para_filiais, estoque, left_on='sigla_estoque', right_on='sigla_filial')

estoque = estoque[[
    'id_estoque', 'id_produto', 'id_filial', 'quantidade', 'estoque_minimo', 'estoque_maximo',
    'ultima_entrada', 'ultima_saida', 'lote', 'validade'
]]

estoque.to_csv('Tables/estoques.csv')


# TABELA FORNECEDORES
fornecedores = pd.read_sql('SELECT * FROM fornecedores', engine_estoque)

fornecedores['razao_social'] = fornecedores['razao_social'].str.upper()
fornecedores['nome_fantasia'] = fornecedores['nome_fantasia'].str.upper()
fornecedores['telefone'] = fornecedores['telefone'].astype(str).str.replace(r'\D', '', regex=True)
fornecedores['telefone'] = fornecedores['telefone'].str.replace(
    r'(\d{2})(\d{4,5})(\d{4})', 
    r'(\1) \2-\3', 
    regex=True
)
fornecedores['cidade'] = fornecedores['cidade'].str.upper()
fornecedores['ativo'] = fornecedores['ativo'].map({'ativo': True, 'inativo': False})

fornecedores = fornecedores.rename(columns={'ativo': 'status'})

#fornecedores.to_csv('Tables/fornecedores.csv')


# TABELA ITENS ENTRADA
itens_entrada = pd.read_sql('SELECT * FROM itens_entrada', engine_estoque)

#itens_entrada.to_csv('Tables/itens_entrada.csv')


# TABELA CARGOS
cargos = pd.read_sql('SELECT * FROM cargos', engine_rh)

cargos['nome_cargo'] = cargos['nome_cargo'].str.upper()
cargos['nivel'] = cargos['nivel'].str.upper()

#cargos.to_csv('Tables/cargos.csv')


# TABELA ESCALAS
escalas = pd.read_sql('SELECT * FROM escalas', engine_rh)

escalas['dia_semana'] = escalas['dia_semana'].str.upper()
escalas['tipo_escala'] = escalas['tipo_escala'].str.upper()

escalas = escalas.rename(columns={'matricula': 'id_funcionario'})

#escalas.to_csv('Tables/escalas.csv')


# TABELA HISTORICO SALARIO
historico_salario = pd.read_sql('SELECT * FROM historico_salario', engine_rh)

historico_salario['data_alteracao'] = pd.to_datetime(historico_salario['data_alteracao'])
historico_salario['motivo'] = historico_salario['motivo'].str.upper()

historico_salario = historico_salario.rename(columns={'matricula': 'id_funcionario'})

#historico_salario.to_csv('Tables/historico_salario.csv')


# TABELA FUNCIONARIOS
df_funcionarios = pd.read_sql('SELECT * FROM funcionarios', engine_rh)
df_departamentos = pd.read_sql('SELECT id_departamento, nome_depto FROM departamentos', engine_rh)

funcionarios = pd.merge(df_funcionarios, df_departamentos, on='id_departamento')

funcionarios['nome_completo'] = funcionarios['nome_completo'].str.upper()
funcionarios['nr_cpf'] = funcionarios['nr_cpf'].astype(str).str.replace(
    r'(\d{3})(\d{3})(\d{3})(\d{2})', 
    r'\1.\2.\3-\4', 
    regex=True
)
funcionarios['data_nascimento'] = pd.to_datetime(funcionarios['data_nascimento'])
funcionarios['data_admissao'] = pd.to_datetime(funcionarios['data_admissao'])
funcionarios['data_demissao'] = pd.to_datetime(funcionarios['data_demissao'])
funcionarios['telefone'] = funcionarios['telefone'].astype(str).str.replace(r'\D', '', regex=True)
funcionarios['telefone'] = funcionarios['telefone'].str.replace(
    r'(\d{2})(\d{4,5})(\d{4})', 
    r'(\1) \2-\3', 
    regex=True
)
funcionarios['endereco'] =  funcionarios['endereco'].str.upper()
funcionarios['cep'] = funcionarios['cep'].astype(str).str.replace(r'\D', '', regex=True)
funcionarios['cep'] = funcionarios['cep'].str.replace(
    r'(\d{5})(\d{3})', 
    r'\1-\2', 
    regex=True
)
funcionarios['status'] = funcionarios['status'].map({1: True, 0: False})
funcionarios['nome_depto'] = funcionarios['nome_depto'].str.upper()

funcionarios = funcionarios[[
    'matricula', 'nome_completo', 'nr_cpf', 'data_nascimento', 'data_admissao', 'data_demissao', 
    'id_cargo', 'nome_depto','id_filial_rh', 'salario', 'email', 'telefone', 'endereco', 'cep', 'status'
]]

funcionarios = funcionarios.rename(columns={
    'matricula': 'id_funcionario',
    'nr_cpf': 'cpf_funcionario',
    'nome_cargo': 'cargo',
    'nome_depto': 'departamento',
    'id_filial_rh': 'id_filial'
})

#funcionarios.to_csv('Tables/funcionarios.csv')