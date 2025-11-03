# Construindo Pipelines de Dados Modernos: ETL/ELT com Apache Airflow 3 - Do Conceito à Prática

## Introdução
## Descrição da Aula
Esta aula prática de 3 horas é voltada para desenvolvedores, engenheiros de dados, analistas de dados e estudantes de engenharia de software interessados em automação e orquestração de fluxos de dados. Os participantes aprenderão os conceitos fundamentais de pipelines de dados, as diferenças entre abordagens ETL e ELT, e ganharão experiência prática criando um pipeline completo usando Apache Airflow 3.

## Objetivo da Aula
Compreender conceitos de pipelines de dados automatizados, abordagens ETL e ELT, e criar um pipeline prático usando Apache Airflow 3 com PostgreSQL.

## Arquitetura do Ambiente

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Environment                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   Airflow 3     │    │        PostgreSQL               │ │
│  │   Standalone    │    │      (northwind DB)            │ │
│  │                 │    │                                 │ │
│  │ • Webserver     │◄──►│ • Host: postgres_erp            │ │
│  │ • Scheduler     │    │ • Port: 5432 (internal)        │ │
│  │ • Executor      │    │ • Port: 2001 (external)        │ │
│  │ • Port: 8080    │    │ • User: postgres                │ │
│  └─────────────────┘    │ • Password: postgres            │ │
│           │              └─────────────────────────────────┘ │
│           │                                                  │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                   Volumes                               │ │
│  │ • ./dags → /opt/airflow/dags                           │ │
│  │ • ./data → /opt/airflow/data                           │ │
│  │ • ./logs → /opt/airflow/logs                           │ │
│  │ • ./plugins → /opt/airflow/plugins                     │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    ETL Pipeline Flow                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  CSV File ──► Extract ──► Transform ──► Load ──► PostgreSQL │
│  (dados_vendas.csv)  │         │          │     (vendas)    │
│                      │         │          │                 │
│                      ▼         ▼          ▼                 │
│                   • Read CSV  • Clean    • Create Table     │
│                   • Validate  • Calculate • Insert Data     │
│                   • Log Info  • Aggregate • Verify Load     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Estrutura da Aula

### Conceitos
- **O que são Pipelines de Dados?** - Definição, propósito, componentes típicos (ingestão, transformação, carregamento, orquestração) e importância em ambientes de dados modernos
- **Abordagem ETL (Extract, Transform, Load)** - Processo de extração de fontes diversas, transformação em ambiente staging e carregamento no destino final
- **Abordagem ELT (Extract, Load, Transform)** - Carregamento de dados brutos primeiro e transformação no sistema de destino usando seu poder de processamento
- **Comparação ETL vs ELT** - Vantagens, desvantagens e fatores para escolha entre as abordagens

### Ferramentas
- **Ferramentas de Orquestração (Apache Airflow)** - Plataforma para criar, agendar e monitorar workflows usando DAGs, interface de usuário e escalabilidade
- **Ferramentas de Transformação (DBT, Pandas, Spark)** - DBT para transformações SQL em data warehouses, Pandas para manipulação em memória e Spark para processamento distribuído
- **Ferramentas de Ingestão e Armazenamento** - Apache Kafka, NiFi, Airbyte para ingestão e Data Lakes, Data Warehouses, bancos de dados como destinos

### Laboratório Prático
- **Setup do ambiente** - Configuração do Docker, Airflow 3 e PostgreSQL para execução do pipeline
- **Criação de pipeline ETL com Airflow** - Desenvolvimento hands-on de DAG completa com extração de CSV, transformação de dados e carregamento no PostgreSQL;

### Discussão e Conclusão
- **Revisão do pipeline criado** - Análise do código desenvolvido, benefícios da automação e conceitos de DataOps
- **Orquestração avançada e monitoramento** - Extensões com Airflow para agendamento, dependências, monitoramento visual e tratamento de erros
- **Q&A e próximos passos** - Dúvidas, adaptações para diferentes cenários e recursos para aprofundamento

## Setup do Ambiente

### Pré-requisitos
- Docker e Docker Compose instalados
- Python 3.8+ (para desenvolvimento local)

### Inicialização do Airflow 3

1. **Build da imagem personalizada:**
```bash
docker compose build
```

2. **Inicializar banco de dados:**
```bash
docker compose up --no-deps --wait airflow-init
```

3. **Subir os serviços:**
```bash
docker compose up -d
```

4. **Acessar a interface:**
- URL: http://localhost:8080
- Usuário: `admin`
- Senha: [airflow_token]

## Laboratório Prático

### Cenário
Criar um pipeline ETL que:
1. **Extrai** dados do arquivo CSV `data/dados_vendas.csv`
2. **Transforma** os dados (limpeza e agregação)
3. **Carrega** os dados transformados no PostgreSQL

### Estrutura dos Dados
O arquivo `dados_vendas.csv` contém:
- `ID_Produto`: Identificador do produto
- `Valor`: Preço unitário
- `Quantidade`: Quantidade vendida
- `Data`: Data da venda
- `Regiao`: Região da venda

### DAG Criada
A DAG `etl_vendas_pipeline` executa:
1. **extract_data**: Lê dados do CSV
2. **transform_data**: Limpa dados nulos e calcula total de vendas
3. **load_data**: Carrega dados no PostgreSQL

### Comandos Úteis

**Ver logs:**
```bash
docker compose logs -f airflow-standalone
```

**Parar serviços:**
```bash
docker compose down
```

**Acessar container do Airflow:**
```bash
docker compose exec airflow-standalone bash
```

**Conectar ao PostgreSQL:**
```bash
docker compose exec postgres_erp psql -U postgres -d northwind
```

## Conceitos Importantes

### ETL vs ELT

**ETL (Extract, Transform, Load):**
- Transformação ocorre antes do carregamento
- Ideal para dados estruturados
- Menor uso de recursos no destino

**ELT (Extract, Load, Transform):**
- Dados brutos carregados primeiro
- Transformação no sistema de destino
- Ideal para Big Data e dados semi-estruturados

### Benefícios da Automação
- **Consistência**: Execução padronizada
- **Repetibilidade**: Processos reproduzíveis
- **Monitoramento**: Visibilidade do status
- **Escalabilidade**: Processamento de grandes volumes

## Próximos Passos
- Explorar transformações mais complexas
- Implementar tratamento de erros
- Adicionar testes de qualidade de dados
- Integrar com ferramentas de BI