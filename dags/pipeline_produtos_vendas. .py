from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

# Configuração padrão da DAG
default_args = {
    'owner': 'exercicio_final',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG - Schedule diário às 6h da manhã
dag = DAG(
    'pipeline_produtos_vendas',
    default_args=default_args,
    description='Pipeline ETL completo para processar dados de produtos e vendas',
    schedule_interval='0 6 * * *',  # 6h da manhã todos os dias
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio'],
)

def extract_produtos(**context):
    """Task 1: Extrair dados de produtos_loja.csv"""
    file_path = '/opt/airflow/data/produtos_loja.csv'
    logging.info(f"Iniciando extração de produtos de: {file_path}")
    
    # Validar se o arquivo existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    # Ler arquivo CSV
    df_produtos = pd.read_csv(file_path)
    logging.info(f"Produtos extraídos: {len(df_produtos)} registros")
    
    # Salvar dados extraídos para próxima tarefa
    df_produtos.to_csv('/tmp/produtos_extraidos.csv', index=False)
    
    # Log detalhado dos dados
    logging.info(f"Colunas: {list(df_produtos.columns)}")
    logging.info(f"Produtos por categoria: {df_produtos['Categoria'].value_counts().to_dict()}")
    
    return f"Extraídos {len(df_produtos)} produtos com sucesso"

def extract_vendas(**context):
    """Task 2: Extrair dados de vendas_produtos.csv"""
    file_path = '/opt/airflow/data/vendas_produtos.csv'
    logging.info(f"Iniciando extração de vendas de: {file_path}")
    
    # Validar se o arquivo existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    # Ler arquivo CSV
    df_vendas = pd.read_csv(file_path)
    logging.info(f"Vendas extraídas: {len(df_vendas)} registros")
    
    # Salvar dados extraídos para próxima tarefa
    df_vendas.to_csv('/tmp/vendas_extraidas.csv', index=False)
    
    # Log detalhado dos dados
    logging.info(f"Colunas: {list(df_vendas.columns)}")
    logging.info(f"Vendas por canal: {df_vendas['Canal_Venda'].value_counts().to_dict()}")
    
    return f"Extraídas {len(df_vendas)} vendas com sucesso"

def transform_data(**context):
    """Task 3: Transformar e limpar os dados"""
    logging.info("Iniciando transformação dos dados")
    
    # Carregar dados extraídos
    df_produtos = pd.read_csv('/tmp/produtos_extraidos.csv')
    df_vendas = pd.read_csv('/tmp/vendas_extraidas.csv')
    
    logging.info("Dados carregados para transformação")
    
    # === LIMPEZA DE DADOS ===
    
    # 1. Preencher Preco_Custo nulo com média da categoria
    logging.info("Tratando valores nulos em Preco_Custo")
    df_produtos['Preco_Custo'] = pd.to_numeric(df_produtos['Preco_Custo'], errors='coerce')
    
    # Calcular média por categoria para preenchimento
    media_por_categoria = df_produtos.groupby('Categoria')['Preco_Custo'].mean()
    logging.info(f"Médias por categoria: {media_por_categoria.to_dict()}")
    
    # Preencher valores nulos
    for categoria in df_produtos['Categoria'].unique():
        mask = (df_produtos['Categoria'] == categoria) & (df_produtos['Preco_Custo'].isna())
        if mask.any():
            valor_medio = media_por_categoria[categoria]
            df_produtos.loc[mask, 'Preco_Custo'] = valor_medio
            logging.info(f"Preenchido Preco_Custo para categoria {categoria} com {valor_medio}")
    
    # 2. Preencher Fornecedor nulo com "Não Informado"
    mask_fornecedor = df_produtos['Fornecedor'].isna() | (df_produtos['Fornecedor'] == '')
    df_produtos.loc[mask_fornecedor, 'Fornecedor'] = 'Não Informado'
    logging.info(f"Preenchidos {mask_fornecedor.sum()} fornecedores com 'Não Informado'")
    
    # 3. Preencher Preco_Venda nulo com Preco_Custo * 1.3
    df_vendas['Preco_Venda'] = pd.to_numeric(df_vendas['Preco_Venda'], errors='coerce')
    
    # Merge para obter preço de custo
    df_vendas_merged = df_vendas.merge(df_produtos[['ID_Produto', 'Preco_Custo']], 
                                     on='ID_Produto', how='left')
    
    mask_preco_venda = df_vendas_merged['Preco_Venda'].isna()
    df_vendas_merged.loc[mask_preco_venda, 'Preco_Venda'] = df_vendas_merged.loc[mask_preco_venda, 'Preco_Custo'] * 1.3
    logging.info(f"Preenchidos {mask_preco_venda.sum()} preços de venda com fórmula Preco_Custo * 1.3")
    
    # === TRANSFORMAÇÕES ===
    
    # 4. Calcular Receita_Total = Quantidade_Vendida * Preco_Venda
    df_vendas_merged['Receita_Total'] = df_vendas_merged['Quantidade_Vendida'] * df_vendas_merged['Preco_Venda']
    logging.info("Calculada Receita_Total")
    
    # 5. Calcular Margem_Lucro = Preco_Venda - Preco_Custo
    df_vendas_merged['Margem_Lucro'] = df_vendas_merged['Preco_Venda'] - df_vendas_merged['Preco_Custo']
    logging.info("Calculada Margem_Lucro")
    
    # 6. Criar campo Mes_Venda extraído de Data_Venda
    df_vendas_merged['Data_Venda'] = pd.to_datetime(df_vendas_merged['Data_Venda'])
    df_vendas_merged['Mes_Venda'] = df_vendas_merged['Data_Venda'].dt.strftime('%Y-%m')
    logging.info("Criado campo Mes_Venda")
    
    # Remover coluna auxiliar Preco_Custo das vendas (já está na tabela produtos)
    df_vendas_final = df_vendas_merged.drop('Preco_Custo', axis=1)
    
    # Salvar dados transformados
    df_produtos.to_csv('/tmp/produtos_transformados.csv', index=False)
    df_vendas_final.to_csv('/tmp/vendas_transformadas.csv', index=False)
    
    logging.info("Transformação concluída com sucesso")
    logging.info(f"Produtos processados: {len(df_produtos)}")
    logging.info(f"Vendas processadas: {len(df_vendas_final)}")
    
    return "Dados transformados com sucesso"

# Task 4: Criar tabelas no PostgreSQL
create_tables_sql = """
-- Tabela produtos_processados
DROP TABLE IF EXISTS produtos_processados CASCADE;
CREATE TABLE produtos_processados (
    ID_Produto VARCHAR(10),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Preco_Custo DECIMAL(10,2),
    Fornecedor VARCHAR(100),
    Status VARCHAR(20),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela vendas_processadas
DROP TABLE IF EXISTS vendas_processadas CASCADE;
CREATE TABLE vendas_processadas (
    ID_Venda VARCHAR(10),
    ID_Produto VARCHAR(10),
    Quantidade_Vendida INTEGER,
    Preco_Venda DECIMAL(10,2),
    Data_Venda DATE,
    Canal_Venda VARCHAR(20),
    Receita_Total DECIMAL(10,2),
    Mes_Venda VARCHAR(7),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela relatorio_vendas
DROP TABLE IF EXISTS relatorio_vendas CASCADE;
CREATE TABLE relatorio_vendas (
    ID_Venda VARCHAR(10),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Quantidade_Vendida INTEGER,
    Receita_Total DECIMAL(10,2),
    Margem_Lucro DECIMAL(10,2),
    Canal_Venda VARCHAR(20),
    Mes_Venda VARCHAR(7),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def load_data(**context):
    """Task 5: Carregar dados transformados nas tabelas PostgreSQL"""
    logging.info("Iniciando carregamento dos dados no PostgreSQL")
    
    # Conectar ao PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Carregar dados transformados
    df_produtos = pd.read_csv('/tmp/produtos_transformados.csv')
    df_vendas = pd.read_csv('/tmp/vendas_transformadas.csv')
    
    # Inserir produtos
    logging.info("Inserindo produtos na tabela produtos_processados")
    df_produtos_insert = df_produtos.copy()
    pg_hook.insert_rows(
        table='produtos_processados',
        rows=df_produtos_insert.values.tolist(),
        target_fields=['ID_Produto', 'Nome_Produto', 'Categoria', 'Preco_Custo', 'Fornecedor', 'Status']
    )
    
    # Inserir vendas
    logging.info("Inserindo vendas na tabela vendas_processadas")
    df_vendas_insert = df_vendas[['ID_Venda', 'ID_Produto', 'Quantidade_Vendida', 
                                 'Preco_Venda', 'Data_Venda', 'Canal_Venda', 
                                 'Receita_Total', 'Mes_Venda']].copy()
    pg_hook.insert_rows(
        table='vendas_processadas',
        rows=df_vendas_insert.values.tolist(),
        target_fields=['ID_Venda', 'ID_Produto', 'Quantidade_Vendida', 'Preco_Venda', 
                      'Data_Venda', 'Canal_Venda', 'Receita_Total', 'Mes_Venda']
    )
    
    # Criar relatório consolidado (JOIN)
    logging.info("Criando relatório consolidado")
    join_query = """
    INSERT INTO relatorio_vendas 
    (ID_Venda, Nome_Produto, Categoria, Quantidade_Vendida, Receita_Total, Margem_Lucro, Canal_Venda, Mes_Venda)
    SELECT 
        v.ID_Venda,
        p.Nome_Produto,
        p.Categoria,
        v.Quantidade_Vendida,
        v.Receita_Total,
        (v.Preco_Venda - p.Preco_Custo) as Margem_Lucro,
        v.Canal_Venda,
        v.Mes_Venda
    FROM vendas_processadas v
    JOIN produtos_processados p ON v.ID_Produto = p.ID_Produto;
    """
    pg_hook.run(join_query)
    
    # Validar inserção
    count_produtos = pg_hook.get_first("SELECT COUNT(*) FROM produtos_processados")[0]
    count_vendas = pg_hook.get_first("SELECT COUNT(*) FROM vendas_processadas")[0]
    count_relatorio = pg_hook.get_first("SELECT COUNT(*) FROM relatorio_vendas")[0]
    
    logging.info(f"Dados carregados com sucesso:")
    logging.info(f"- Produtos: {count_produtos} registros")
    logging.info(f"- Vendas: {count_vendas} registros")
    logging.info(f"- Relatório: {count_relatorio} registros")
    
    if count_produtos != len(df_produtos) or count_vendas != len(df_vendas):
        raise ValueError("Erro na validação: número de registros inseridos não confere")
    
    return f"Dados carregados: {count_produtos} produtos, {count_vendas} vendas, {count_relatorio} relatórios"

def generate_report(**context):
    """Task 6: Gerar relatório com estatísticas"""
    logging.info("Gerando relatório de vendas")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 1. Total de vendas por categoria
    vendas_categoria = pg_hook.get_records("""
        SELECT Categoria, 
               SUM(Receita_Total) as Total_Vendas,
               SUM(Quantidade_Vendida) as Total_Quantidade
        FROM relatorio_vendas 
        GROUP BY Categoria 
        ORDER BY Total_Vendas DESC
    """)
    
    logging.info("=== TOTAL DE VENDAS POR CATEGORIA ===")
    for categoria, total_vendas, total_qtd in vendas_categoria:
        logging.info(f"{categoria}: R$ {total_vendas:.2f} ({total_qtd} unidades)")
    
    # 2. Produto mais vendido
    produto_mais_vendido = pg_hook.get_first("""
        SELECT Nome_Produto, 
               SUM(Quantidade_Vendida) as Total_Vendido,
               SUM(Receita_Total) as Total_Receita
        FROM relatorio_vendas 
        GROUP BY Nome_Produto 
        ORDER BY Total_Vendido DESC 
        LIMIT 1
    """)
    
    logging.info("=== PRODUTO MAIS VENDIDO ===")
    logging.info(f"Produto: {produto_mais_vendido[0]}")
    logging.info(f"Quantidade: {produto_mais_vendido[1]} unidades")
    logging.info(f"Receita: R$ {produto_mais_vendido[2]:.2f}")
    
    # 3. Canal de venda com maior receita
    canal_maior_receita = pg_hook.get_first("""
        SELECT Canal_Venda, 
               SUM(Receita_Total) as Total_Receita,
               COUNT(*) as Num_Vendas
        FROM relatorio_vendas 
        GROUP BY Canal_Venda 
        ORDER BY Total_Receita DESC 
        LIMIT 1
    """)
    
    logging.info("=== CANAL COM MAIOR RECEITA ===")
    logging.info(f"Canal: {canal_maior_receita[0]}")
    logging.info(f"Receita: R$ {canal_maior_receita[1]:.2f}")
    logging.info(f"Número de vendas: {canal_maior_receita[2]}")
    
    # 4. Margem de lucro média por categoria
    margem_categoria = pg_hook.get_records("""
        SELECT Categoria, 
               AVG(Margem_Lucro) as Margem_Media,
               COUNT(*) as Num_Vendas
        FROM relatorio_vendas 
        GROUP BY Categoria 
        ORDER BY Margem_Media DESC
    """)
    
    logging.info("=== MARGEM DE LUCRO MÉDIA POR CATEGORIA ===")
    for categoria, margem_media, num_vendas in margem_categoria:
        logging.info(f"{categoria}: R$ {margem_media:.2f} (em {num_vendas} vendas)")
    
    # Resumo geral
    resumo = pg_hook.get_first("""
        SELECT 
            COUNT(*) as Total_Vendas,
            SUM(Receita_Total) as Receita_Total,
            AVG(Margem_Lucro) as Margem_Media_Geral
        FROM relatorio_vendas
    """)
    
    logging.info("=== RESUMO GERAL ===")
    logging.info(f"Total de vendas: {resumo[0]}")
    logging.info(f"Receita total: R$ {resumo[1]:.2f}")
    logging.info(f"Margem média: R$ {resumo[2]:.2f}")
    
    return "Relatório gerado com sucesso"

# Definição das tarefas
extract_produtos_task = PythonOperator(
    task_id='extract_produtos',
    python_callable=extract_produtos,
    dag=dag,
)

extract_vendas_task = PythonOperator(
    task_id='extract_vendas',
    python_callable=extract_vendas,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql=create_tables_sql,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Definição das dependências
[extract_produtos_task, extract_vendas_task] >> transform_data_task >> create_tables_task >> load_data_task >> generate_report_task