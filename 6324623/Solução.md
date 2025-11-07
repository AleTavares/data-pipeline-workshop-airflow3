# Instruções finais para o exercício - Pipeline de Dados de Produtos

Arquivos gerados (pasta /mnt/data/pipeline_outputs):
- produtos_processados.csv
- vendas_processadas.csv
- relatorio_vendas.csv
- produtos_baixa_performance.csv
- relatorio_analitico.json

## Como validar localmente (sem Airflow)
1. Verifique os CSVs gerados na pasta acima.
2. Rode queries no PostgreSQL (se estiver usando) para conferir contagens e amostras.

Exemplos de queries para validação:

```sql
SELECT COUNT(*) FROM produtos_processados;
SELECT COUNT(*) FROM vendas_processadas;
SELECT COUNT(*) FROM relatorio_vendas;
SELECT Categoria, SUM(Receita_Total) FROM relatorio_vendas GROUP BY Categoria;
SELECT Nome_Produto, SUM(Quantidade_Vendida) FROM relatorio_vendas GROUP BY Nome_Produto ORDER BY SUM(Quantidade_Vendida) DESC LIMIT 5;
```

## Como deployar a DAG no Airflow
1. Copie `pipeline_produtos_vendas_dag.py` para o diretório de DAGs do Airflow.
2. Ajuste `DATA_DIR` no topo do arquivo para apontar para onde os CSVs estarão.
3. Configure a connection `postgres_default` no Admin > Connections.
4. Coloque os arquivos `produtos_loja.csv` e `vendas_produtos.csv` na pasta configurada.
5. No UI do Airflow, ative e execute a DAG manualmente. Acompanhe logs em cada task.

## Docker-compose (opcional) - Postgres mínimo para testes
Crie um `docker-compose.yml` com Postgres e inicialize:

```yaml
version: '3.7'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
volumes:
  pgdata:
```

Depois, configure a connection no Airflow com host `localhost`, porta `5432`, usuário `airflow`, senha `airflow`, database `airflow`.

---

Gerado em: 2025-11-07T16:17:16.528603 UTC
