# README - pipeline_produtos_vendas

Resumo

Este DAG implementa um pipeline ETL para processar os arquivos `data/produtos_loja.csv` e `data/vendas_produtos.csv`, gerando tabelas processadas e um relatório de vendas em PostgreSQL.

Arquivos importantes

- `dags/pipeline_produtos_vendas.py` - DAG implementada com TaskFlow API.
- `data/produtos_loja.csv` e `data/vendas_produtos.csv` - dados de entrada (já presentes no repositório).

Justificativa ETL vs ELT

Escolhi ETL (extrair → transformar em memória com pandas → carregar) porque:
- Conjunto de dados é pequeno (CSV sample) e cabe na memória.
- As transformações envolvem lógica de negócio e imputação que é simples de aplicar com pandas.
- Esse formato facilita validação e testes locais antes do carregamento no banco.

Pré-requisitos

- Docker + Docker Compose (recomendado: uso do `docker compose up -d` a partir da raiz do projeto).
- Airflow (standalone via compose no repositório deve subir o webserver e um Postgres para testes).
- Se executar localmente (sem Docker): Python, pandas, apache-airflow e apache-airflow-providers-postgres.

Como rodar (Docker Compose)

1. Na raiz do projeto:

```powershell
# gerar .env com chaves (exemplo)
python -c "import base64,os; print('AIRFLOW_FERNET_KEY='+base64.urlsafe_b64encode(os.urandom(32)).decode()); print('AIRFLOW_SECRET_KEY=change_this_secret')" > .env

# subir containers (pode demorar na primeira vez)
docker compose up -d
```

2. Verificar containers:

```powershell
docker compose ps
```

3. Abrir UI do Airflow (ex.: http://localhost:5000) e logar com o usuário `admin` e a senha exibida nos logs do container standalone.

4. Localize a DAG `pipeline_produtos_vendas`, despause se necessário e acione manualmente (Trigger). Acompanhe logs por task na interface.

Alternativa CLI (via container):

```powershell
# listar dags
docker compose exec airflow-standalone airflow dags list

# trigger manual
docker compose exec airflow-standalone airflow dags trigger pipeline_produtos_vendas
```

O que a DAG faz (resumo técnico)

- extract_produtos: lê `produtos_loja.csv`, valida existência e retorna registros por XCom.
- extract_vendas: lê `vendas_produtos.csv`, valida existência e retorna registros por XCom.
- transform_data: aplica imputações (Preco_Custo média por categoria, Fornecedor 'Não Informado', Preco_Venda = Preco_Custo * 1.3 quando ausente), calcula Receita_Total, Margem_Lucro, Mes_Venda.
- create_tables: cria `produtos_processados`, `vendas_processadas` e `relatorio_vendas` no Postgres.
- load_data: truncagem das tabelas (idempotência), inserção em batch e validação pós-load (contagens). A task falha se a validação informar divergência.
- generate_report: executa queries resumidas (vendas por categoria, produto mais vendido, canal top, margem média por categoria).
- produtos_baixa_performance (bônus): detecta produtos com soma de vendas < 2, cria `produtos_baixa_performance` e insere os records.

Notas operacionais e recomendações

- Idempotência: a implementação atual TRUNCATE antes do insert — isso evita duplicação em re-runs, mas remove histórico. Se preferir manter histórico, implementar UPSERT ou uma abordagem de staging.
- XCom: dados são passados via XCom serializados em JSON — adequado para datasets pequenos. Para produção com dados grandes use armazenamento intermediário (S3, GCS, ou DB staging).
- Conexão Postgres: o DAG usa `postgres_default`. Configure essa conexão no Airflow UI (Admin → Connections) se necessário.
- Email on failure: definido como `False` no DAG por ser requisito do exercício.

Validação manual no banco

Após execução, conecte-se ao Postgres (mapeado no compose) e execute:

```sql
SELECT COUNT(*) FROM produtos_processados;
SELECT COUNT(*) FROM vendas_processadas;
SELECT COUNT(*) FROM relatorio_vendas;
SELECT * FROM produtos_baixa_performance;
```

Entrega

Se precisar que eu crie a pasta de entrega `[SEU_RA]/` com a DAG e README, me informe o seu RA e eu a adiciono.
