from __future__ import annotations

import logging
from pathlib import Path
from datetime import datetime
from typing import Tuple

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

DATA_DIR = Path('/opt/airflow/data') 
PRODUTOS_CSV = DATA_DIR / 'produtos_loja.csv'
VENDAS_CSV = DATA_DIR / 'vendas_produtos.csv'
POSTGRES_CONN_ID = 'postgres_default' 

default_args = {
    'owner': 'airflow',
    'retries': 2,
}

with DAG(
    dag_id='pipeline_produtos_vendas',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 6 * * *',  
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio'],
) as dag:

    @task
    def extract_produtos() -> pd.DataFrame:
        logging.info('Iniciando extract_produtos')
        if not PRODUTOS_CSV.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {PRODUTOS_CSV}")
        df = pd.read_csv(PRODUTOS_CSV)
        logging.info(f'produtos extraídos: {len(df)} registros')
        return df

    @task
    def extract_vendas() -> pd.DataFrame:
        logging.info('Iniciando extract_vendas')
        if not VENDAS_CSV.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {VENDAS_CSV}")
        df = pd.read_csv(VENDAS_CSV, parse_dates=['Data_Venda'])
        logging.info(f'vendas extraídas: {len(df)} registros')
        return df

    @task
    def transform_data(produtos: pd.DataFrame, vendas: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logging.info('Iniciando transform_data')

        produtos_clean = produtos.copy()
        produtos_clean['Fornecedor'] = produtos_clean['Fornecedor'].fillna('Não Informado')

        produtos_clean['Preco_Custo'] = pd.to_numeric(produtos_clean['Preco_Custo'], errors='coerce')

        media_categoria = produtos_clean.groupby('Categoria')['Preco_Custo'].transform('mean')
        produtos_clean['Preco_Custo'] = produtos_clean['Preco_Custo'].fillna(media_categoria)

        if produtos_clean['Preco_Custo'].isna().any():
            global_mean = produtos_clean['Preco_Custo'].mean()
            produtos_clean['Preco_Custo'] = produtos_clean['Preco_Custo'].fillna(global_mean)

        vendas_clean = vendas.copy()
        vendas_clean['Quantidade_Vendida'] = pd.to_numeric(vendas_clean['Quantidade_Vendida'], downcast='integer', errors='coerce').fillna(0).astype(int)
        vendas_clean['Preco_Venda'] = pd.to_numeric(vendas_clean['Preco_Venda'], errors='coerce')

        vendas_join = vendas_clean.merge(produtos_clean[['ID_Produto', 'Preco_Custo', 'Nome_Produto', 'Categoria']], on='ID_Produto', how='left')

        vendas_join['Preco_Venda'] = vendas_join.apply(
            lambda row: row['Preco_Venda'] if pd.notna(row['Preco_Venda']) else (row['Preco_Custo'] * 1.3 if pd.notna(row['Preco_Custo']) else None),
            axis=1,
        )

        if vendas_join['Preco_Venda'].isna().any():
            logging.warning('Algumas vendas não possuem Preco_Venda e nem Preco_Custo; preenchendo com 0')
            vendas_join['Preco_Venda'] = vendas_join['Preco_Venda'].fillna(0)

        vendas_join['Receita_Total'] = vendas_join['Quantidade_Vendida'] * vendas_join['Preco_Venda']
        vendas_join['Margem_Lucro'] = vendas_join['Preco_Venda'] - vendas_join['Preco_Custo']
        vendas_join['Mes_Venda'] = vendas_join['Data_Venda'].dt.to_period('M').astype(str)

        produtos_processados = produtos_clean.copy()
        vendas_processadas = vendas_join[['ID_Venda', 'ID_Produto', 'Quantidade_Vendida', 'Preco_Venda', 'Data_Venda', 'Canal_Venda', 'Receita_Total', 'Mes_Venda']].copy()

        relatorio = vendas_join[['ID_Venda', 'Nome_Produto', 'Categoria', 'Quantidade_Vendida', 'Receita_Total', 'Margem_Lucro', 'Canal_Venda', 'Mes_Venda']].copy()

        logging.info('Transformação concluída')
        return produtos_processados, vendas_processadas, relatorio

    @task
    def create_tables() -> None:
        logging.info('Criando tabelas no PostgreSQL (se não existirem)')
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute('''
        CREATE TABLE IF NOT EXISTS produtos_processados (
            ID_Produto VARCHAR(10) PRIMARY KEY,
            Nome_Produto VARCHAR(100),
            Categoria VARCHAR(50),
            Preco_Custo NUMERIC(10,2),
            Fornecedor VARCHAR(100),
            Status VARCHAR(20),
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ''')

        cur.execute('''
        CREATE TABLE IF NOT EXISTS vendas_processadas (
            ID_Venda VARCHAR(10) PRIMARY KEY,
            ID_Produto VARCHAR(10),
            Quantidade_Vendida INTEGER,
            Preco_Venda NUMERIC(10,2),
            Data_Venda DATE,
            Canal_Venda VARCHAR(20),
            Receita_Total NUMERIC(12,2),
            Mes_Venda VARCHAR(7),
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ''')

        cur.execute('''
        CREATE TABLE IF NOT EXISTS relatorio_vendas (
            ID_Venda VARCHAR(10) PRIMARY KEY,
            Nome_Produto VARCHAR(100),
            Categoria VARCHAR(50),
            Quantidade_Vendida INTEGER,
            Receita_Total NUMERIC(12,2),
            Margem_Lucro NUMERIC(10,2),
            Canal_Venda VARCHAR(20),
            Mes_Venda VARCHAR(7),
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ''')

        cur.execute('''
        CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
            ID_Produto VARCHAR(10) PRIMARY KEY,
            Nome_Produto VARCHAR(100),
            Total_Vendas INTEGER,
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ''')

        conn.commit()
        cur.close()
        conn.close()
        logging.info('Tabelas criadas/confirmadas')

    @task
    def load_data(produtos_df: pd.DataFrame, vendas_df: pd.DataFrame, relatorio_df: pd.DataFrame) -> None:
        logging.info('Iniciando carga de dados no PostgreSQL')
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        produtos_records = produtos_df.replace({pd.NA: None}).to_dict(orient='records')
        produtos_tuples = [(
            r.get('ID_Produto'), r.get('Nome_Produto'), r.get('Categoria'),
            float(r.get('Preco_Custo')) if r.get('Preco_Custo') is not None else None,
            r.get('Fornecedor'), r.get('Status')
        ) for r in produtos_records]

        cur.executemany(
            '''INSERT INTO produtos_processados (ID_Produto, Nome_Produto, Categoria, Preco_Custo, Fornecedor, Status)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT (ID_Produto) DO UPDATE SET
                 Nome_Produto = EXCLUDED.Nome_Produto,
                 Categoria = EXCLUDED.Categoria,
                 Preco_Custo = EXCLUDED.Preco_Custo,
                 Fornecedor = EXCLUDED.Fornecedor,
                 Status = EXCLUDED.Status;
            ''',
            produtos_tuples
        )

        vendas_records = vendas_df.replace({pd.NA: None}).to_dict(orient='records')
        vendas_tuples = [(
            r.get('ID_Venda'), r.get('ID_Produto'), int(r.get('Quantidade_Vendida')),
            float(r.get('Preco_Venda')) if r.get('Preco_Venda') is not None else 0.0,
            r.get('Data_Venda').date() if pd.notnull(r.get('Data_Venda')) else None,
            r.get('Canal_Venda'), float(r.get('Receita_Total')) if r.get('Receita_Total') is not None else 0.0,
            r.get('Mes_Venda')
        ) for r in vendas_records]

        cur.executemany(
            '''INSERT INTO vendas_processadas (ID_Venda, ID_Produto, Quantidade_Vendida, Preco_Venda, Data_Venda, Canal_Venda, Receita_Total, Mes_Venda)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (ID_Venda) DO UPDATE SET
                 ID_Produto = EXCLUDED.ID_Produto,
                 Quantidade_Vendida = EXCLUDED.Quantidade_Vendida,
                 Preco_Venda = EXCLUDED.Preco_Venda,
                 Data_Venda = EXCLUDED.Data_Venda,
                 Canal_Venda = EXCLUDED.Canal_Venda,
                 Receita_Total = EXCLUDED.Receita_Total,
                 Mes_Venda = EXCLUDED.Mes_Venda;
            ''',
            vendas_tuples
        )

        relatorio_records = relatorio_df.replace({pd.NA: None}).to_dict(orient='records')
        relatorio_tuples = [(
            r.get('ID_Venda'), r.get('Nome_Produto'), r.get('Categoria'), int(r.get('Quantidade_Vendida')),
            float(r.get('Receita_Total')) if r.get('Receita_Total') is not None else 0.0,
            float(r.get('Margem_Lucro')) if r.get('Margem_Lucro') is not None else 0.0,
            r.get('Canal_Venda'), r.get('Mes_Venda')
        ) for r in relatorio_records]

        cur.executemany(
            '''INSERT INTO relatorio_vendas (ID_Venda, Nome_Produto, Categoria, Quantidade_Vendida, Receita_Total, Margem_Lucro, Canal_Venda, Mes_Venda)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (ID_Venda) DO UPDATE SET
                 Nome_Produto = EXCLUDED.Nome_Produto,
                 Categoria = EXCLUDED.Categoria,
                 Quantidade_Vendida = EXCLUDED.Quantidade_Vendida,
                 Receita_Total = EXCLUDED.Receita_Total,
                 Margem_Lucro = EXCLUDED.Margem_Lucro,
                 Canal_Venda = EXCLUDED.Canal_Venda,
                 Mes_Venda = EXCLUDED.Mes_Venda;
            ''',
            relatorio_tuples
        )

        conn.commit()
        cur.close()
        conn.close()
        logging.info('Carga concluída e validada por commit')

    @task
    def generate_report() -> dict:
        logging.info('Gerando relatório analítico')
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute('SELECT Categoria, SUM(Receita_Total) FROM relatorio_vendas GROUP BY Categoria ORDER BY SUM(Receita_Total) DESC;')
        vendas_por_categoria = cur.fetchall()

        cur.execute('SELECT Nome_Produto, SUM(Quantidade_Vendida) as total_qt FROM relatorio_vendas GROUP BY Nome_Produto ORDER BY total_qt DESC LIMIT 1;')
        produto_mais_vendido = cur.fetchone()

        cur.execute('SELECT Canal_Venda, SUM(Receita_Total) as receita FROM relatorio_vendas GROUP BY Canal_Venda ORDER BY receita DESC LIMIT 1;')
        canal_maior_receita = cur.fetchone()

        cur.execute('SELECT Categoria, AVG(Margem_Lucro) FROM relatorio_vendas GROUP BY Categoria;')
        margem_media = cur.fetchall()

        cur.close()
        conn.close()

        report = {
            'vendas_por_categoria': vendas_por_categoria,
            'produto_mais_vendido': produto_mais_vendido,
            'canal_maior_receita': canal_maior_receita,
            'margem_media_por_categoria': margem_media,
        }

        logging.info('Relatório gerado')
        logging.info(report)
        return report

    @task
    def bonus_detect_low_performance(vendas_df: pd.DataFrame) -> None:
        logging.info('Detectando produtos com baixa performance')
        counts = vendas_df.groupby('ID_Produto')['Quantidade_Vendida'].sum().reset_index()
        low_perf = counts[counts['Quantidade_Vendida'] < 2]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        for _, row in low_perf.iterrows():
            cur.execute('''
                INSERT INTO produtos_baixa_performance (ID_Produto, Nome_Produto, Total_Vendas)
                VALUES (%s, %s, %s)
                ON CONFLICT (ID_Produto) DO UPDATE SET Total_Vendas = EXCLUDED.Total_Vendas;
            ''', (row['ID_Produto'], None, int(row['Quantidade_Vendida'])))
            logging.warning(f'Produto com baixa performance detectado: {row["ID_Produto"]} - total vendas {row["Quantidade_Vendida"]}')

        conn.commit()
        cur.close()
        conn.close()
        logging.info('Detecção de baixa performance concluída')

    produtos_df = extract_produtos()
    vendas_df = extract_vendas()
    produtos_proc, vendas_proc, relatorio_df = transform_data(produtos_df, vendas_df)

    create_tables_task = create_tables()
    load = load_data(produtos_proc, vendas_proc, relatorio_df)
    report = generate_report()
    bonus = bonus_detect_low_performance(vendas_proc)

    create_tables_task >> load >> report
    load >> bonus
