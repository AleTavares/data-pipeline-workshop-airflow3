# 🚀 Construindo Pipelines de Dados Modernos: ETL/ELT com Apache Airflow 3

## 📑 Introdução

Bem-vindo à aula prática focada em **Automação e Orquestração de Fluxos de Dados** utilizando ferramentas modernas de Engenharia de Dados. Neste laboratório de 3 horas, transformaremos dados brutos de vendas em informações estruturadas, criando um pipeline completo com Apache Airflow 3 e PostgreSQL.

### 🎯 Objetivo da Aula

* Compreender os conceitos de pipelines de dados automatizados e as abordagens **ETL** e **ELT**.
* Ganhar experiência prática desenvolvendo uma **DAG (Directed Acyclic Graph)** completa para processamento de dados.
* Dominar o uso de `PythonOperator` e `PostgresOperator` no Apache Airflow.

---

## 🏗️ Arquitetura do Ambiente (Docker Compose)

Nosso ambiente é totalmente conteinerizado, garantindo que todos os participantes usem o mesmo setup.

| Componente | Função | Detalhes de Acesso |
| :--- | :--- | :--- |
| **Airflow 3 Standalone** | Orquestração (Webserver, Scheduler, Executor) | URL: `http://localhost:8080` |
| **PostgreSQL** | Base de Dados de Destino (`northwind` DB) | Host: `postgres_erp` (interno), Porta Externa: `2001` |

### Mapeamento de Volumes

| Diretório Local | Diretório no Container | Propósito |
| :--- | :--- | :--- |
| `./dags` | `/opt/airflow/dags` | Armazenamento dos arquivos `.py` da DAG. |
| `./data` | `/opt/airflow/data` | Fonte de dados (Ex: `dados_vendas.csv`). |
| `./logs` | `/opt/airflow/logs` | Logs de execução do Airflow. |

### Fluxo do Pipeline ETL

O pipeline simula um fluxo básico, indo do arquivo CSV para a tabela final no banco de dados.
CSV File ──► Extract ──► Transform ──► Load ──► PostgreSQL (dados_vendas.csv)


---

## 📖 Estrutura da Aula

### 1. Conceitos Fundamentais
* **O que são Pipelines de Dados?** Definição, propósito e componentes (ingestão, transformação, orquestração).
* **Abordagens de Transformação:**
    * **ETL (Extract, Transform, Load):** Foco em transformar em ambiente *staging* antes de carregar.
    * **ELT (Extract, Load, Transform):** Foco em carregar dados brutos primeiro e usar o poder do destino para transformar.
* **Comparação e Escolha:** Vantagens, desvantagens e fatores decisivos (Volume de Dados, Complexidade da Transformação, Capacidade do Destino).

### 2. Ferramentas no Ecossistema de Dados
* **Orquestração:** Apache Airflow (DAGs, Scheduler, UI).
* **Transformação:** Pandas (manipulação em memória), DBT (transformações SQL), Spark (processamento distribuído).
* **Ingestão e Armazenamento:** Apache Kafka, Airbyte, Data Warehouses (Ex: Snowflake, BigQuery).

### 3. Laboratório Prático: Airflow Hands-on
* **Setup do Ambiente:** Inicialização do Docker e acesso ao Airflow UI.
* **Desenvolvimento da DAG:** Criação da DAG `etl_vendas_pipeline`.
* **Implementação das Tasks:**
    1.  `extract_data`: Leitura e validação do CSV.
    2.  `transform_data`: Limpeza de nulos e cálculo de métricas (`Valor * Quantidade`).
    3.  `load_data`: Criação da tabela e inserção dos dados no PostgreSQL.

---

## 💻 Setup do Ambiente (Passos Práticos)

Para iniciar o laboratório, siga os passos abaixo no seu terminal:

### Pré-requisitos
* Docker e Docker Compose instalados.
* Python 3.8+ (para desenvolvimento local de DAGs).

### Inicialização
1.  **Build da Imagem (Primeira vez):**
    ```bash
    docker compose build
    ```

2.  **Inicialização e Setup do Airflow (Inicialização de DB e Usuário):**
    ```bash
    docker compose up --no-deps --wait airflow-init
    ```

3.  **Subir os Serviços (Airflow e PostgreSQL):**
    ```bash
    docker compose up -d
    ```

### Acesso e Credenciais
* **Airflow UI:** `http://localhost:8080`
* **Usuário/Senha:** `admin` / `[airflow_token]` (Verifique o token no log de inicialização se o `airflow-init` não o definir automaticamente).

---

## 🧪 Laboratório Prático: Cenário e Dados

### Cenário ETL

Criaremos o pipeline ETL para processar dados de vendas do arquivo `data/dados_vendas.csv` e carregar na tabela `vendas` do PostgreSQL.

### Estrutura dos Dados (`dados_vendas.csv`)

| Campo | Descrição |
| :--- | :--- |
| `ID_Produto` | Identificador do Produto |
| `Valor` | Preço unitário da venda |
| `Quantidade` | Quantidade vendida na transação |
| `Data` | Data da transação |
| `Regiao` | Região de origem da venda |

### Comandos Úteis

| Comando | Descrição |
| :--- | :--- |
| **Ver Logs do Airflow:** | `docker compose logs -f airflow-standalone` |
| **Parar Serviços:** | `docker compose down` |
| **Acessar Container do Airflow:** | `docker compose exec airflow-standalone bash` |
| **Conectar ao PostgreSQL:** | `docker compose exec postgres_erp psql -U postgres -d northwind` |

---

## 💡 Conceitos Importantes para Avaliação

### ETL vs ELT (Revisão Rápida)

* **ETL:** Transformação pesada em *staging* (Ex: Pandas). Bom para dados estruturados, baixo custo de computação no destino.
* **ELT:** Transformação no Data Warehouse (Ex: dbt/SQL). Bom para Big Data, aproveita a escalabilidade do destino.

### Benefícios da Automação

* **Consistência e Repetibilidade:** Resultados idênticos em toda execução.
* **Monitoramento:** Visibilidade instantânea do status do pipeline e tratamento de erros.
* **Escalabilidade:** Capacidade de processar volumes crescentes de dados sem intervenção manual.

---

## ▶️ Próximos Passos (Discussão)

Ao final do laboratório, discutiremos:
1.  **Revisão do Código:** Onde e como o Pandas foi usado para garantir a qualidade dos dados.
2.  **Monitoramento:** Como usar o Airflow UI para acompanhar e depurar.
3.  **DataOps:** A importância de automação, testes e tratamento de erros para a operação de dados moderna.
4.  **Extensões:** Implementação de transformações mais complexas, testes de qualidade de dados e integração com ferramentas de BI.
