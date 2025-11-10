from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.decorators import task
# days_ago foi removido/alterado em versões mais novas do Airflow; usar uma data fixa de início
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOG = logging.getLogger("pipeline_produtos_vendas")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "email_on_failure": False,
}


def _project_root() -> Path:
    # this file is in <project>/dags/, project root is parent
    return Path(__file__).resolve().parent.parent


with DAG(
    dag_id="pipeline_produtos_vendas",
    default_args=default_args,
    description="Pipeline ETL de produtos e vendas",
    schedule="0 6 * * *",
    # start_date fixa (UTC) para compatibilidade com diferentes versões do Airflow
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["produtos", "vendas", "exercicio"],
) as dag:

    @task(task_id="extract_produtos")
    def extract_produtos() -> str:
        root = _project_root()
        path = root / "data" / "produtos_loja.csv"
        if not path.exists():
            LOG.error("Arquivo produtos_loja.csv não encontrado em %s", path)
            raise FileNotFoundError(f"{path} não encontrado")

        df = pd.read_csv(path)
        LOG.info("produtos extraídos: %d registros", len(df))
        # serializar para JSON para XCom
        return df.to_json(orient="records", date_format="iso")

    @task(task_id="extract_vendas")
    def extract_vendas() -> str:
        root = _project_root()
        path = root / "data" / "vendas_produtos.csv"
        if not path.exists():
            LOG.error("Arquivo vendas_produtos.csv não encontrado em %s", path)
            raise FileNotFoundError(f"{path} não encontrado")

        df = pd.read_csv(path)
        LOG.info("vendas extraídas: %d registros", len(df))
        return df.to_json(orient="records", date_format="iso")

    @task(task_id="transform_data")
    def transform_data(produtos_json: str, vendas_json: str) -> dict:
        # carregar
        prod = pd.read_json(produtos_json, orient="records")
        vend = pd.read_json(vendas_json, orient="records")

        # normalizar colunas
        prod_cols = ["ID_Produto", "Nome_Produto", "Categoria", "Preco_Custo", "Fornecedor", "Status"]
        vend_cols = ["ID_Venda", "ID_Produto", "Quantidade_Vendida", "Preco_Venda", "Data_Venda", "Canal_Venda"]

        prod = prod.reindex(columns=prod_cols)
        vend = vend.reindex(columns=vend_cols)

        # Conversões de tipos
        prod["Preco_Custo"] = pd.to_numeric(prod["Preco_Custo"], errors="coerce")
        vend["Preco_Venda"] = pd.to_numeric(vend["Preco_Venda"], errors="coerce")
        vend["Quantidade_Vendida"] = pd.to_numeric(vend["Quantidade_Vendida"], errors="coerce").fillna(0).astype(int)

        # Preencher Preco_Custo nulo com média da categoria
        prod["Preco_Custo"] = prod.groupby("Categoria")["Preco_Custo"].transform(lambda s: s.fillna(s.mean()))

        # Preencher Fornecedor nulo
        prod["Fornecedor"] = prod["Fornecedor"].fillna("Não Informado")

        # Garantir que Preco_Custo não esteja nulo (se categoria inteira nula, usar mediana global)
        if prod["Preco_Custo"].isna().any():
            global_mean = prod["Preco_Custo"].mean()
            prod["Preco_Custo"] = prod["Preco_Custo"].fillna(global_mean)

        # Preencher Preco_Venda nulo com Preco_Custo * 1.3
        # primeiro unir Preco_Custo nos registros de vendas
        vend = vend.merge(prod[["ID_Produto", "Preco_Custo"]], on="ID_Produto", how="left")
        vend["Preco_Venda"] = vend.apply(lambda r: r["Preco_Venda"] if pd.notna(r["Preco_Venda"]) else (r["Preco_Custo"] * 1.3 if pd.notna(r["Preco_Custo"]) else None), axis=1)

        # Se ainda houver Preco_Venda nulo, preencher com 0
        vend["Preco_Venda"] = vend["Preco_Venda"].fillna(0)

        # Calcular Receita_Total e Margem_Lucro
        vend["Receita_Total"] = vend["Quantidade_Vendida"] * vend["Preco_Venda"]
        vend = vend.merge(prod[["ID_Produto", "Nome_Produto", "Categoria", "Preco_Custo"]], on="ID_Produto", how="left")
        vend["Margem_Lucro"] = vend["Preco_Venda"] - vend["Preco_Custo"]

        # Criar Mes_Venda a partir de Data_Venda
        vend["Data_Venda"] = pd.to_datetime(vend["Data_Venda"], errors="coerce")
        vend["Mes_Venda"] = vend["Data_Venda"].dt.strftime("%Y-%m")

        # Ajustar tipos e colunas finais
        produtos_processados = prod.copy()
        vendas_processadas = vend[["ID_Venda", "ID_Produto", "Quantidade_Vendida", "Preco_Venda", "Data_Venda", "Canal_Venda", "Receita_Total", "Mes_Venda"]].copy()

        # relatorio: join
        rel = vendas_processadas.merge(produtos_processados[["ID_Produto", "Nome_Produto", "Categoria", "Preco_Custo"]], on="ID_Produto", how="left")
        rel["Margem_Lucro"] = rel["Preco_Venda"] - rel["Preco_Custo"]

        LOG.info("transformação concluída: produtos=%d vendas=%d", len(produtos_processados), len(vendas_processadas))

        # Serializar para XCom (pequeno dataset) - usamos orient='records'
        return {
            "produtos": produtos_processados.to_json(orient="records", date_format="iso"),
            "vendas": vendas_processadas.to_json(orient="records", date_format="iso"),
            "relatorio": rel.to_json(orient="records", date_format="iso"),
        }

    @task(task_id="create_tables")
    def create_tables() -> None:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql_produtos = """
        CREATE TABLE IF NOT EXISTS produtos_processados (
            ID_Produto VARCHAR(10),
            Nome_Produto VARCHAR(100),
            Categoria VARCHAR(50),
            Preco_Custo DECIMAL(10,2),
            Fornecedor VARCHAR(100),
            Status VARCHAR(20),
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        sql_vendas = """
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
        """

        sql_rel = """
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
        """

        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql_produtos)
        cur.execute(sql_vendas)
        cur.execute(sql_rel)
        conn.commit()
        cur.close()
        LOG.info("Tabelas criadas ou já existentes: produtos_processados, vendas_processadas, relatorio_vendas")

    @task(task_id="load_data")
    def load_data(payload: dict) -> None:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cur = conn.cursor()

        produtos = pd.read_json(payload["produtos"], orient="records")
        vendas = pd.read_json(payload["vendas"], orient="records")
        rel = pd.read_json(payload["relatorio"], orient="records")

        # Idempotência simples: truncar as tabelas antes de inserir
        cur.execute("TRUNCATE TABLE produtos_processados RESTART IDENTITY;")
        cur.execute("TRUNCATE TABLE vendas_processadas RESTART IDENTITY;")
        cur.execute("TRUNCATE TABLE relatorio_vendas RESTART IDENTITY;")
        conn.commit()

        # Inserir produtos
        insert_prod = "INSERT INTO produtos_processados (ID_Produto, Nome_Produto, Categoria, Preco_Custo, Fornecedor, Status) VALUES (%s,%s,%s,%s,%s,%s)"
        prod_tuples = [(
            row.ID_Produto,
            row.Nome_Produto,
            row.Categoria,
            None if pd.isna(row.Preco_Custo) else float(row.Preco_Custo),
            row.Fornecedor if "Fornecedor" in row.index else None,
            row.Status if "Status" in row.index else None,
        ) for row in produtos.itertuples(index=False)]

        if prod_tuples:
            cur.executemany(insert_prod, prod_tuples)

        # Inserir vendas
        insert_vend = "INSERT INTO vendas_processadas (ID_Venda, ID_Produto, Quantidade_Vendida, Preco_Venda, Data_Venda, Canal_Venda, Receita_Total, Mes_Venda) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        vend_tuples = []
        for row in vendas.itertuples(index=False):
            data_venda = row.Data_Venda
            if pd.isna(data_venda):
                data_venda = None
            else:
                # converter para date
                data_venda = pd.to_datetime(data_venda).date()
            vend_tuples.append((
                row.ID_Venda,
                row.ID_Produto,
                int(row.Quantidade_Vendida) if not pd.isna(row.Quantidade_Vendida) else 0,
                None if pd.isna(row.Preco_Venda) else float(row.Preco_Venda),
                data_venda,
                row.Canal_Venda,
                None if pd.isna(row.Receita_Total) else float(row.Receita_Total),
                row.Mes_Venda,
            ))

        if vend_tuples:
            cur.executemany(insert_vend, vend_tuples)

        # Inserir relatorio
        insert_rel = "INSERT INTO relatorio_vendas (ID_Venda, Nome_Produto, Categoria, Quantidade_Vendida, Receita_Total, Margem_Lucro, Canal_Venda, Mes_Venda) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        rel_tuples = [(
            row.ID_Venda,
            row.Nome_Produto,
            row.Categoria,
            int(row.Quantidade_Vendida) if not pd.isna(row.Quantidade_Vendida) else 0,
            None if pd.isna(row.Receita_Total) else float(row.Receita_Total),
            None if pd.isna(row.Margem_Lucro) else float(row.Margem_Lucro),
            row.Canal_Venda,
            row.Mes_Venda,
        ) for row in rel.itertuples(index=False)]

        if rel_tuples:
            cur.executemany(insert_rel, rel_tuples)

        conn.commit()

        # Validação pós-load: comparar contagens
        cur.execute("SELECT COUNT(*) FROM produtos_processados")
        cnt_prod = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM vendas_processadas")
        cnt_vend = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM relatorio_vendas")
        cnt_rel = cur.fetchone()[0]

        cur.close()

        LOG.info("Dados carregados em Postgres: produtos_db=%d produtos_df=%d | vendas_db=%d vendas_df=%d | rel_db=%d rel_df=%d", cnt_prod, len(prod_tuples), cnt_vend, len(vend_tuples), cnt_rel, len(rel_tuples))

        # lançar erro se houver discrepância
        if cnt_prod != len(prod_tuples) or cnt_vend != len(vend_tuples) or cnt_rel != len(rel_tuples):
            raise Exception(f"Validação pós-load falhou: produtos({cnt_prod}!={len(prod_tuples)}), vendas({cnt_vend}!={len(vend_tuples)}), rel({cnt_rel}!={len(rel_tuples)})")

    @task(task_id="generate_report")
    def generate_report() -> dict:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cur = conn.cursor()

        # Total de vendas por categoria
        cur.execute("SELECT categoria, SUM(receita_total) as total_receita FROM relatorio_vendas GROUP BY categoria ORDER BY total_receita DESC")
        vendas_por_categoria = cur.fetchall()

        # Produto mais vendido (por quantidade)
        cur.execute("SELECT Nome_Produto, SUM(Quantidade_Vendida) as total_qtd FROM relatorio_vendas GROUP BY Nome_Produto ORDER BY total_qtd DESC LIMIT 1")
        produto_mais_vendido = cur.fetchone()

        # Canal de venda com maior receita
        cur.execute("SELECT Canal_Venda, SUM(Receita_Total) as total_receita FROM relatorio_vendas GROUP BY Canal_Venda ORDER BY total_receita DESC LIMIT 1")
        canal_top = cur.fetchone()

        # Margem de lucro média por categoria
        cur.execute("SELECT categoria, AVG(margem_lucro) as margem_media FROM relatorio_vendas GROUP BY categoria")
        margem_por_categoria = cur.fetchall()

        cur.close()

        report = {
            "vendas_por_categoria": vendas_por_categoria,
            "produto_mais_vendido": produto_mais_vendido,
            "canal_top": canal_top,
            "margem_por_categoria": margem_por_categoria,
        }

        LOG.info("Relatório gerado: %s", {k: (v if v else "vazio") for k, v in report.items()})
        return report

    @task(task_id="produtos_baixa_performance")
    def produtos_baixa_performance() -> None:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cur = conn.cursor()

        # detectar produtos com menos de 2 vendas
        cur.execute("SELECT ID_Produto, SUM(Quantidade_Vendida) as total_vendas FROM vendas_processadas GROUP BY ID_Produto HAVING SUM(Quantidade_Vendida) < 2")
        fracos = cur.fetchall()

        if fracos:
            LOG.warning("Produtos com baixa performance detectados: %s", fracos)
            # criar tabela
            cur.execute("CREATE TABLE IF NOT EXISTS produtos_baixa_performance (ID_Produto VARCHAR(10), Total_Vendas INTEGER, Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            insert_sql = "INSERT INTO produtos_baixa_performance (ID_Produto, Total_Vendas) VALUES (%s,%s)"
            cur.executemany(insert_sql, [(r[0], int(r[1])) for r in fracos])
            conn.commit()
        else:
            LOG.info("Nenhum produto com baixa performance encontrado")

        cur.close()

    # DAG dependencies
    p = extract_produtos()
    v = extract_vendas()
    transformed = transform_data(p, v)
    t_create = create_tables()
    t_load = load_data(transformed)
    t_report = generate_report()
    t_bonus = produtos_baixa_performance()

    # encadeamento
    p >> v >> transformed
    transformed >> t_create >> t_load >> t_report
    t_load >> t_bonus
