from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import logging
import os

# Configurações padrão da DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'pipeline_produtos_vendas',
    default_args=default_args,
    description='Pipeline ETL para processar dados de produtos e vendas',
    schedule_interval='0 6 * * *',  # Diário às 6h da manhã
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio'],
)

# Função para extrair produtos
def extract_produtos(**context):
    """
    Extrai dados do arquivo produtos_loja.csv
    """
    try:
        file_path = '/opt/airflow/dags/data/produtos_loja.csv'
        
        # Validar se arquivo existe
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        # Ler arquivo CSV
        df_produtos = pd.read_csv(file_path)
        
        # Log do número de registros
        num_registros = len(df_produtos)
        logging.info(f"✅ Extraídos {num_registros} registros de produtos")
        
        # Salvar no XCom para próximas tarefas
        context['task_instance'].xcom_push(key='produtos_data', value=df_produtos.to_dict())
        
        return f"Sucesso: {num_registros} produtos extraídos"
        
    except Exception as e:
        logging.error(f"❌ Erro na extração de produtos: {str(e)}")
        raise

# Função para extrair vendas
def extract_vendas(**context):
    """
    Extrai dados do arquivo vendas_produtos.csv
    """
    try:
        file_path = '/opt/airflow/dags/data/vendas_produtos.csv'
        
        # Validar se arquivo existe
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        # Ler arquivo CSV
        df_vendas = pd.read_csv(file_path)
        
        # Log do número de registros
        num_registros = len(df_vendas)
        logging.info(f"✅ Extraídos {num_registros} registros de vendas")
        
        # Salvar no XCom
        context['task_instance'].xcom_push(key='vendas_data', value=df_vendas.to_dict())
        
        return f"Sucesso: {num_registros} vendas extraídas"
        
    except Exception as e:
        logging.error(f"❌ Erro na extração de vendas: {str(e)}")
        raise

# Função para transformar dados
def transform_data(**context):
    """
    Aplica transformações e limpeza nos dados
    """
    try:
        # Recuperar dados do XCom
        produtos_dict = context['task_instance'].xcom_pull(task_ids='extract_produtos', key='produtos_data')
        vendas_dict = context['task_instance'].xcom_pull(task_ids='extract_vendas', key='vendas_data')
        
        df_produtos = pd.DataFrame(produtos_dict)
        df_vendas = pd.DataFrame(vendas_dict)
        
        logging.info("🔄 Iniciando transformações...")
        
        # TRANSFORMAÇÕES EM PRODUTOS
        # 1. Calcular média de preço por categoria para preencher nulos
        preco_medio_categoria = df_produtos.groupby('Categoria')['Preco_Custo'].mean()
        
        # 2. Preencher Preco_Custo nulo com média da categoria
        for idx, row in df_produtos.iterrows():
            if pd.isna(row['Preco_Custo']):
                categoria = row['Categoria']
                if categoria in preco_medio_categoria.index:
                    df_produtos.at[idx, 'Preco_Custo'] = preco_medio_categoria[categoria]
                    logging.info(f"  → Preço do produto {row['ID_Produto']} preenchido com média da categoria: {preco_medio_categoria[categoria]:.2f}")
        
        # 3. Preencher Fornecedor nulo
        df_produtos['Fornecedor'] = df_produtos['Fornecedor'].fillna('Não Informado')
        
        # TRANSFORMAÇÕES EM VENDAS
        # 1. Merge para obter Preco_Custo
        df_vendas = df_vendas.merge(df_produtos[['ID_Produto', 'Preco_Custo']], on='ID_Produto', how='left')
        
        # 2. Preencher Preco_Venda nulo com Preco_Custo * 1.3
        mask_preco_nulo = df_vendas['Preco_Venda'].isna()
        df_vendas.loc[mask_preco_nulo, 'Preco_Venda'] = df_vendas.loc[mask_preco_nulo, 'Preco_Custo'] * 1.3
        
        # 3. Calcular Receita_Total
        df_vendas['Receita_Total'] = df_vendas['Quantidade_Vendida'] * df_vendas['Preco_Venda']
        
        # 4. Calcular Margem_Lucro
        df_vendas['Margem_Lucro'] = df_vendas['Preco_Venda'] - df_vendas['Preco_Custo']
        
        # 5. Converter Data_Venda para datetime e extrair Mes_Venda
        df_vendas['Data_Venda'] = pd.to_datetime(df_vendas['Data_Venda'])
        df_vendas['Mes_Venda'] = df_vendas['Data_Venda'].dt.strftime('%Y-%m')
        
        # Criar dataframe para relatório (join dos dados)
        df_relatorio = df_vendas.merge(
            df_produtos[['ID_Produto', 'Nome_Produto', 'Categoria']], 
            on='ID_Produto', 
            how='left'
        )
        
        # Selecionar colunas relevantes para o relatório
        df_relatorio = df_relatorio[[
            'ID_Venda', 'Nome_Produto', 'Categoria', 'Quantidade_Vendida',
            'Receita_Total', 'Margem_Lucro', 'Canal_Venda', 'Mes_Venda'
        ]]
        
        # Remover coluna Preco_Custo do df_vendas antes de salvar
        df_vendas = df_vendas.drop('Preco_Custo', axis=1)
        
        # Salvar dados transformados no XCom
        context['task_instance'].xcom_push(key='produtos_transformed', value=df_produtos.to_dict())
        context['task_instance'].xcom_push(key='vendas_transformed', value=df_vendas.to_dict())
        context['task_instance'].xcom_push(key='relatorio_data', value=df_relatorio.to_dict())
        
        logging.info(f"✅ Transformações concluídas:")
        logging.info(f"  → {len(df_produtos)} produtos processados")
        logging.info(f"  → {len(df_vendas)} vendas processadas")
        logging.info(f"  → {len(df_relatorio)} registros no relatório")
        
        return "Transformações concluídas com sucesso"
        
    except Exception as e:
        logging.error(f"❌ Erro na transformação: {str(e)}")
        raise

# Função para carregar dados
def load_data(**context):
    """
    Carrega dados transformados no PostgreSQL
    """
    try:
        # Recuperar dados transformados
        produtos_dict = context['task_instance'].xcom_pull(task_ids='transform_data', key='produtos_transformed')
        vendas_dict = context['task_instance'].xcom_pull(task_ids='transform_data', key='vendas_transformed')
        relatorio_dict = context['task_instance'].xcom_pull(task_ids='transform_data', key='relatorio_data')
        
        df_produtos = pd.DataFrame(produtos_dict)
        df_vendas = pd.DataFrame(vendas_dict)
        df_relatorio = pd.DataFrame(relatorio_dict)
        
        # Conectar ao PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        logging.info("📤 Carregando dados no PostgreSQL...")
        
        # Limpar tabelas existentes
        cursor.execute("TRUNCATE TABLE produtos_processados, vendas_processadas, relatorio_vendas;")
        
        # Carregar produtos
        for _, row in df_produtos.iterrows():
            cursor.execute("""
                INSERT INTO produtos_processados 
                (ID_Produto, Nome_Produto, Categoria, Preco_Custo, Fornecedor, Status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['ID_Produto'], row['Nome_Produto'], row['Categoria'],
                row['Preco_Custo'], row['Fornecedor'], row['Status']
            ))
        
        # Carregar vendas
        for _, row in df_vendas.iterrows():
            cursor.execute("""
                INSERT INTO vendas_processadas 
                (ID_Venda, ID_Produto, Quantidade_Vendida, Preco_Venda, 
                 Data_Venda, Canal_Venda, Receita_Total, Mes_Venda)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['ID_Venda'], row['ID_Produto'], row['Quantidade_Vendida'],
                row['Preco_Venda'], row['Data_Venda'], row['Canal_Venda'],
                row['Receita_Total'], row['Mes_Venda']
            ))
        
        # Carregar relatório
        for _, row in df_relatorio.iterrows():
            cursor.execute("""
                INSERT INTO relatorio_vendas 
                (ID_Venda, Nome_Produto, Categoria, Quantidade_Vendida,
                 Receita_Total, Margem_Lucro, Canal_Venda, Mes_Venda)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['ID_Venda'], row['Nome_Produto'], row['Categoria'],
                row['Quantidade_Vendida'], row['Receita_Total'], row['Margem_Lucro'],
                row['Canal_Venda'], row['Mes_Venda']
            ))
        
        conn.commit()
        
        # Validar inserção
        cursor.execute("SELECT COUNT(*) FROM produtos_processados")
        count_produtos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM vendas_processadas")
        count_vendas = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM relatorio_vendas")
        count_relatorio = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logging.info(f"✅ Dados carregados com sucesso:")
        logging.info(f"  → {count_produtos} produtos")
        logging.info(f"  → {count_vendas} vendas")
        logging.info(f"  → {count_relatorio} registros no relatório")
        
        return "Dados carregados com sucesso"
        
    except Exception as e:
        logging.error(f"❌ Erro no carregamento: {str(e)}")
        raise

# Função para gerar relatório
def generate_report(**context):
    """
    Gera relatórios analíticos com os dados processados
    """
    try:
        # Conectar ao PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        
        logging.info("📊 Gerando relatórios analíticos...")
        
        # 1. Total de vendas por categoria
        query_categoria = """
            SELECT 
                Categoria,
                SUM(Quantidade_Vendida) as Total_Quantidade,
                SUM(Receita_Total) as Total_Receita,
                AVG(Margem_Lucro) as Margem_Media
            FROM relatorio_vendas
            GROUP BY Categoria
            ORDER BY Total_Receita DESC
        """
        df_categoria = pd.read_sql(query_categoria, conn)
        
        logging.info("\n📈 RELATÓRIO: Vendas por Categoria")
        logging.info("=" * 60)
        for _, row in df_categoria.iterrows():
            logging.info(f"Categoria: {row['categoria']}")
            logging.info(f"  → Quantidade Total: {row['total_quantidade']}")
            logging.info(f"  → Receita Total: R$ {row['total_receita']:.2f}")
            logging.info(f"  → Margem Média: R$ {row['margem_media']:.2f}")
            logging.info("-" * 40)
        
        # 2. Produto mais vendido
        query_top_produto = """
            SELECT 
                Nome_Produto,
                SUM(Quantidade_Vendida) as Total_Vendido,
                SUM(Receita_Total) as Total_Receita
            FROM relatorio_vendas
            GROUP BY Nome_Produto
            ORDER BY Total_Vendido DESC
            LIMIT 1
        """
        df_top_produto = pd.read_sql(query_top_produto, conn)
        
        logging.info("\n🏆 PRODUTO MAIS VENDIDO")
        logging.info("=" * 60)
        if not df_top_produto.empty:
            row = df_top_produto.iloc[0]
            logging.info(f"Produto: {row['nome_produto']}")
            logging.info(f"Quantidade Total: {row['total_vendido']} unidades")
            logging.info(f"Receita Total: R$ {row['total_receita']:.2f}")
        
        # 3. Canal de venda com maior receita
        query_canal = """
            SELECT 
                Canal_Venda,
                COUNT(DISTINCT ID_Venda) as Num_Vendas,
                SUM(Quantidade_Vendida) as Total_Quantidade,
                SUM(Receita_Total) as Total_Receita
            FROM relatorio_vendas
            GROUP BY Canal_Venda
            ORDER BY Total_Receita DESC
        """
        df_canal = pd.read_sql(query_canal, conn)
        
        logging.info("\n🛒 ANÁLISE POR CANAL DE VENDA")
        logging.info("=" * 60)
        for _, row in df_canal.iterrows():
            logging.info(f"Canal: {row['canal_venda']}")
            logging.info(f"  → Número de Vendas: {row['num_vendas']}")
            logging.info(f"  → Quantidade Total: {row['total_quantidade']}")
            logging.info(f"  → Receita Total: R$ {row['total_receita']:.2f}")
            logging.info("-" * 40)
        
        # 4. Margem de lucro média por categoria
        query_margem = """
            SELECT 
                Categoria,
                AVG(Margem_Lucro) as Margem_Media,
                MIN(Margem_Lucro) as Margem_Minima,
                MAX(Margem_Lucro) as Margem_Maxima
            FROM relatorio_vendas
            GROUP BY Categoria
            ORDER BY Margem_Media DESC
        """
        df_margem = pd.read_sql(query_margem, conn)
        
        logging.info("\n💰 MARGEM DE LUCRO POR CATEGORIA")
        logging.info("=" * 60)
        for _, row in df_margem.iterrows():
            logging.info(f"Categoria: {row['categoria']}")
            logging.info(f"  → Margem Média: R$ {row['margem_media']:.2f}")
            logging.info(f"  → Margem Mínima: R$ {row['margem_minima']:.2f}")
            logging.info(f"  → Margem Máxima: R$ {row['margem_maxima']:.2f}")
            logging.info("-" * 40)
        
        conn.close()
        
        logging.info("\n✅ Relatórios gerados com sucesso!")
        
        return "Relatórios gerados com sucesso"
        
    except Exception as e:
        logging.error(f"❌ Erro na geração de relatórios: {str(e)}")
        raise

# DESAFIO BÔNUS: Detectar produtos com baixa performance
def detect_low_performance(**context):
    """
    Detecta produtos com baixa performance (menos de 2 vendas)
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        logging.info("🔍 Analisando performance de produtos...")
        
        # Query para identificar produtos com baixa performance
        query_performance = """
            SELECT 
                p.ID_Produto,
                p.Nome_Produto,
                p.Categoria,
                p.Preco_Custo,
                COALESCE(v.total_vendas, 0) as Total_Vendas,
                COALESCE(v.quantidade_total, 0) as Quantidade_Total
            FROM produtos_processados p
            LEFT JOIN (
                SELECT 
                    ID_Produto,
                    COUNT(*) as total_vendas,
                    SUM(Quantidade_Vendida) as quantidade_total
                FROM vendas_processadas
                GROUP BY ID_Produto
            ) v ON p.ID_Produto = v.ID_Produto
            WHERE COALESCE(v.total_vendas, 0) < 2
        """
        
        df_baixa_perf = pd.read_sql(query_performance, conn)
        
        if not df_baixa_perf.empty:
            logging.warning(f"⚠️ ALERTA: {len(df_baixa_perf)} produtos com baixa performance detectados!")
            
            # Criar tabela se não existir
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
                    ID_Produto VARCHAR(10),
                    Nome_Produto VARCHAR(100),
                    Categoria VARCHAR(50),
                    Preco_Custo DECIMAL(10,2),
                    Total_Vendas INTEGER,
                    Quantidade_Total INTEGER,
                    Data_Analise TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Limpar tabela anterior
            cursor.execute("TRUNCATE TABLE produtos_baixa_performance")
            
            # Inserir produtos com baixa performance
            for _, row in df_baixa_perf.iterrows():
                logging.warning(f"  → {row['nome_produto']}: apenas {row['total_vendas']} venda(s)")
                
                cursor.execute("""
                    INSERT INTO produtos_baixa_performance 
                    (ID_Produto, Nome_Produto, Categoria, Preco_Custo, Total_Vendas, Quantidade_Total)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row['id_produto'], row['nome_produto'], row['categoria'],
                    row['preco_custo'], row['total_vendas'], row['quantidade_total']
                ))
            
            conn.commit()
            logging.info(f"✅ Tabela produtos_baixa_performance atualizada com {len(df_baixa_perf)} registros")
        else:
            logging.info("✅ Todos os produtos têm performance adequada!")
        
        cursor.close()
        conn.close()
        
        return f"Análise concluída: {len(df_baixa_perf)} produtos com baixa performance"
        
    except Exception as e:
        logging.error(f"❌ Erro na detecção de baixa performance: {str(e)}")
        raise

# Definição das tarefas
task_extract_produtos = PythonOperator(
    task_id='extract_produtos',
    python_callable=extract_produtos,
    dag=dag,
)

task_extract_vendas = PythonOperator(
    task_id='extract_vendas',
    python_callable=extract_vendas,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

task_create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
        -- Criar tabela produtos_processados
        CREATE TABLE IF NOT EXISTS produtos_processados (
            ID_Produto VARCHAR(10),
            Nome_Produto VARCHAR(100),
            Categoria VARCHAR(50),
            Preco_Custo DECIMAL(10,2),
            Fornecedor VARCHAR(100),
            Status VARCHAR(20),
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Criar tabela vendas_processadas
        CREATE TABLE IF NOT EXISTS vendas_processadas (
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

        -- Criar tabela relatorio_vendas
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
    """,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

task_bonus = PythonOperator(
    task_id='detect_low_performance',
    python_callable=detect_low_performance,
    dag=dag,
)

# Definir dependências
[task_extract_produtos, task_extract_vendas] >> task_transform >> task_create_tables >> task_load >> task_report >> task_bonus
