from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import psycopg2


# CONFIGURAÇÕES E CAMINHOS

DATA_PATH = '/opt/airflow/data'
PRODUTOS_CSV = os.path.join(DATA_PATH, 'produtos_loja.csv')
VENDAS_CSV = os.path.join(DATA_PATH, 'vendas_produtos.csv')

DB_CONFIG = {
    'host': 'postgres',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

# EXTRAÇÃO DE DADOS

def extract_produtos():
    if not os.path.exists(PRODUTOS_CSV):
        raise FileNotFoundError("Arquivo produtos_loja.csv não encontrado!")
    df_prod = pd.read_csv(PRODUTOS_CSV)
    print(f"✅ Produtos extraídos: {len(df_prod)} registros")
    return df_prod.to_json()

def extract_vendas():
    if not os.path.exists(VENDAS_CSV):
        raise FileNotFoundError("Arquivo vendas_produtos.csv não encontrado!")
    df_vendas = pd.read_csv(VENDAS_CSV)
    print(f"✅ Vendas extraídas: {len(df_vendas)} registros")
    return df_vendas.to_json()

# TRANSFORMAÇÃO DE DADOS

def transform_data(ti):
    df_prod = pd.read_json(ti.xcom_pull(task_ids='extract_produtos'))
    df_vendas = pd.read_json(ti.xcom_pull(task_ids='extract_vendas'))

    # Preencher Preco_Custo nulo com média da categoria
    df_prod['Preco_Custo'] = df_prod.groupby('Categoria')['Preco_Custo'].transform(
        lambda x: x.fillna(x.mean())
    )

    # Preencher Fornecedor nulo com "Não Informado"
    df_prod['Fornecedor'] = df_prod['Fornecedor'].fillna('Não Informado')

    # Preencher Preco_Venda nulo com custo * 1.3
    df_vendas = df_vendas.merge(df_prod[['ID_Produto', 'Preco_Custo']], on='ID_Produto', how='left')
    df_vendas['Preco_Venda'] = df_vendas.apply(
        lambda x: x['Preco_Venda'] if pd.notnull(x['Preco_Venda']) else x['Preco_Custo'] * 1.3,
        axis=1
    )

    # Criar colunas derivadas
    df_vendas['Receita_Total'] = df_vendas['Quantidade_Vendida'] * df_vendas['Preco_Venda']
    df_vendas['Margem_Lucro'] = df_vendas['Preco_Venda'] - df_vendas['Preco_Custo']
    df_vendas['Mes_Venda'] = pd.to_datetime(df_vendas['Data_Venda']).dt.to_period('M').astype(str)

    print("✅ Transformação concluída!")

    return {
        'produtos': df_prod.to_json(),
        'vendas': df_vendas.to_json()
    }


# CRIAÇÃO DAS TABELAS
def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS produtos_processados (
        ID_Produto VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Preco_Custo DECIMAL(10,2),
        Fornecedor VARCHAR(100),
        Status VARCHAR(20),
        Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS vendas_processadas (
        ID_Venda VARCHAR(10),
        ID_Produto VARCHAR(10),
        Quantidade_Vendida INTEGER,
        Preco_Venda DECIMAL(10,2),
        Data_Venda DATE,
        Canal_Venda VARCHAR(20),
        Receita_Total DECIMAL(10,2),
        Margem_Lucro DECIMAL(10,2),
        Mes_Venda VARCHAR(7),
        Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS relatorio_vendas (
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
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Tabelas criadas com sucesso!")


# CARREGAMENTO DOS DADOS

def load_data(ti):
    df_prod = pd.read_json(ti.xcom_pull(task_ids='transform_data')['produtos'])
    df_vendas = pd.read_json(ti.xcom_pull(task_ids='transform_data')['vendas'])

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df_prod.iterrows():
        cur.execute("""
            INSERT INTO produtos_processados 
            (ID_Produto, Nome_Produto, Categoria, Preco_Custo, Fornecedor, Status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row.ID_Produto, row.Nome_Produto, row.Categoria, row.Preco_Custo, row.Fornecedor, row.Status))

    for _, row in df_vendas.iterrows():
        cur.execute("""
            INSERT INTO vendas_processadas 
            (ID_Venda, ID_Produto, Quantidade_Vendida, Preco_Venda, Data_Venda, Canal_Venda, Receita_Total, Margem_Lucro, Mes_Venda)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row.ID_Venda, row.ID_Produto, row.Quantidade_Vendida, row.Preco_Venda, row.Data_Venda,
              row.Canal_Venda, row.Receita_Total, row.Margem_Lucro, row.Mes_Venda))

    conn.commit()
    cur.close()
    conn.close()
    print("Dados carregados no banco!")


# RELATÓRIO

def generate_report():
    conn = psycopg2.connect(**DB_CONFIG)
    df_vendas = pd.read_sql("SELECT * FROM vendas_processadas", conn)
    df_prod = pd.read_sql("SELECT * FROM produtos_processados", conn)
    conn.close()

    relatorio = df_vendas.merge(df_prod, on='ID_Produto', how='left')
    resumo = {
        "Total por Categoria": relatorio.groupby('Categoria')['Receita_Total'].sum().to_dict(),
        "Produto mais vendido": relatorio.groupby('Nome_Produto')['Quantidade_Vendida'].sum().idxmax(),
        "Canal com maior receita": relatorio.groupby('Canal_Venda')['Receita_Total'].sum().idxmax(),
        "Margem média por Categoria": relatorio.groupby('Categoria')['Margem_Lucro'].mean().round(2).to_dict()
    }

    print("RELATÓRIO GERADO:")
    print(resumo)

# BÔNUS: BAIXA PERFORMANCE
def baixa_performance():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT ID_Produto, Quantidade_Vendida FROM vendas_processadas", conn)

    df_resumo = df.groupby('ID_Produto', as_index=False)['Quantidade_Vendida'].sum()
    df_baixa = df_resumo[df_resumo['Quantidade_Vendida'] < 2]

    if df_baixa.empty:
        print("Nenhum produto de baixa performance encontrado.")
    else:
        print("Produtos de baixa performance detectados:")
        print(df_baixa)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
                    ID_Produto VARCHAR(10),
                    Quantidade_Vendida INTEGER,
                    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

            cur.execute("DELETE FROM produtos_baixa_performance;")

            for _, row in df_baixa.iterrows():
                cur.execute("""
                    INSERT INTO produtos_baixa_performance (ID_Produto, Quantidade_Vendida)
                    VALUES (%s, %s)
                """, (row['ID_Produto'], int(row['Quantidade_Vendida'])))
            conn.commit()

    conn.close()
    print("Tarefa de baixa performance concluída!")


# DEFINIÇÃO DA DAG

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'email_on_failure': False,
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id='pipeline_produtos_vendas',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio']
) as dag:

    t1 = PythonOperator(task_id='extract_produtos', python_callable=extract_produtos)
    t2 = PythonOperator(task_id='extract_vendas', python_callable=extract_vendas)
    t3 = PythonOperator(task_id='transform_data', python_callable=transform_data)
    t4 = PythonOperator(task_id='create_tables', python_callable=create_tables)
    t5 = PythonOperator(task_id='load_data', python_callable=load_data)
    t6 = PythonOperator(task_id='generate_report', python_callable=generate_report)
    t7 = PythonOperator(task_id='baixa_performance', python_callable=baixa_performance)

    # Ordem de execução
    [t1, t2] >> t3 >> t4 >> t5 >> t6 >> t7
