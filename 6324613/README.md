# Entrega - RA 6324613 - Vinícius Caires De Souza

Arquivos incluídos:

- `pipeline_produtos_vendas.py` - DAG implementada (cópia do `dags/pipeline_produtos_vendas.py`).
- `README.md` - este arquivo com instruções de entrega.

Instruções de execução (resumo)

1. Coloque esta pasta na raiz do repositório (já está em `6324613/`).
2. Certifique-se de que os arquivos de dados estão em `data/` na raiz do projeto (`produtos_loja.csv` e `vendas_produtos.csv`).
3. Suba o ambiente Airflow + Postgres (recomendado via Docker Compose presente no repositório):

```powershell
docker compose up -d
```

4. Abra a UI do Airflow (ex.: http://localhost:5000), localize a DAG `pipeline_produtos_vendas` e dispare manualmente (Trigger).

Observações

- A DAG utiliza a conexão Airflow `postgres_default`. Garanta que ela exista e aponte para o Postgres do ambiente.
- O carregamento é idempotente (o DAG trunca as tabelas antes de inserir). Se preferir outra estratégia, editar o código.
- Para entrega, enviar PR com esta pasta `6324613/` na raiz do repositório.


-- Vinícius Caires De Souza (RA 6324613)
