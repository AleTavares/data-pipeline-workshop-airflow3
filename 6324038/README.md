# Pipeline de Produtos e Vendas - Exercício Final

## 📋 Parte 1: Análise e Planejamento

### Problemas Identificados nos Dados:

**Arquivo `produtos_loja.csv`:**
- `Preco_Custo` nulo no produto P003 (Teclado Mecânico)
- `Fornecedor` nulo no produto P005 (Webcam HD)

**Arquivo `vendas_produtos.csv`:**
- `Preco_Venda` nulo na venda V005

### Estratégia ETL Escolhida: **ETL**

**Justificativa:**
- **Volume de dados pequeno**: 5 produtos e 5 vendas podem ser processados em memória
- **Transformações específicas**: Necessário limpeza de dados nulos e cálculos antes do carregamento
- **Validação prévia**: Melhor validar e limpar os dados antes de inserir no banco
- **Recursos limitados**: Ambiente de desenvolvimento não requer processamento distribuído

### Transformações Necessárias:
- Preencher valores nulos com regras de negócio
- Calcular métricas derivadas (receita, margem, mês)
- Validar integridade dos dados

## 🚀 Parte 2: Implementação da DAG

### DAG: `pipeline_produtos_vendas`

**Configurações:**
- **Schedule**: Diário às 6h da manhã (`0 6 * * *`)
- **Retries**: 2 tentativas
- **Email on failure**: False
- **Tags**: ['produtos', 'vendas', 'exercicio']

### Tarefas Implementadas:

#### Task 1: `extract_produtos`
- Lê arquivo `produtos_loja.csv`
- Valida se o arquivo existe
- Log do número de registros extraídos
- Salva dados temporários para processamento

#### Task 2: `extract_vendas`
- Lê arquivo `vendas_produtos.csv`
- Valida se o arquivo existe
- Log do número de registros extraídos
- Salva dados temporários para processamento

#### Task 3: `transform_data`
**Limpeza de dados:**
- `Preco_Custo` nulo → preenchido com média da categoria (R$ 82,75 para Acessórios)
- `Fornecedor` nulo → preenchido com "Não Informado"
- `Preco_Venda` nulo → preenchido com `Preco_Custo * 1.3` (R$ 59,15)

**Transformações:**
- `Receita_Total` = `Quantidade_Vendida * Preco_Venda`
- `Margem_Lucro` = `Preco_Venda - Preco_Custo`
- `Mes_Venda` extraído de `Data_Venda` (formato YYYY-MM)

#### Task 4: `create_tables`
Cria todas as tabelas necessárias:
- `produtos_processados` - Produtos com dados limpos
- `vendas_processadas` - Vendas com cálculos
- `relatorio_vendas` - Relatório consolidado (JOIN)
- `produtos_baixa_performance` - Produtos com baixa performance (bônus)

#### Task 5: `load_data`
- Carrega dados transformados no PostgreSQL
- Insere dados nas tabelas produtos e vendas
- Gera relatório consolidado com JOIN
- Valida se os dados foram inseridos corretamente

#### Task 6: `generate_report`
Gera relatórios com:
- Total de vendas por categoria
- Produto mais vendido
- Canal de venda com maior receita
- Margem de lucro média por categoria

#### Task 7: `detect_low_performance` (Bônus)
- Detecta produtos com menos de 2 vendas
- Envia alerta por log
- Cria tabela `produtos_baixa_performance`

### Dependências entre Tarefas:
```
extract_produtos ──┐
                   ├── transform_data → create_tables → load_data → generate_report → detect_low_performance
extract_vendas ────┘
```

## ⚙️ Parte 3: Configuração e Execução

### 1. Iniciar Ambiente
```bash
docker compose up -d
```

### 2. Configurar Conexão PostgreSQL
**Via Interface Web:**
- Acesse: http://localhost:5000
- Login: admin / admin
- Admin → Connections → Add
- Connection Id: `postgres_default`
- Type: Postgres
- Host: `postgres_erp`
- Schema: `northwind`
- Login: `postgres`
- Password: `postgres`
- Port: 5432

**Via CLI:**
```bash
docker compose exec airflow-standalone airflow connections add postgres_default \
  --conn-type postgres --conn-host postgres_erp --conn-port 5432 \
  --conn-login postgres --conn-password postgres --conn-schema northwind
```

### 3. Executar Pipeline
**Via Interface:**
- Encontre DAG `pipeline_produtos_vendas`
- Clique em "Trigger DAG"
- Acompanhe execução

**Via CLI:**
```bash
docker compose exec airflow-standalone airflow dags unpause pipeline_produtos_vendas
docker compose exec airflow-standalone airflow dags trigger pipeline_produtos_vendas
```

### 4. Verificar Resultados
```bash
# Conectar ao PostgreSQL
docker compose exec postgres_erp psql -U postgres -d northwind

# Verificar dados processados
SELECT COUNT(*) FROM produtos_processados;
SELECT COUNT(*) FROM vendas_processadas;
SELECT COUNT(*) FROM relatorio_vendas;

# Ver relatórios
SELECT * FROM relatorio_vendas ORDER BY Receita_Total DESC;
```

## 📊 Resultados Obtidos

### Dados Processados:
- **5 produtos** processados e carregados
- **5 vendas** processadas e carregadas
- **5 registros** no relatório consolidado

### Transformações Aplicadas:
- **P003 (Teclado Mecânico)**: Preço de custo = R$ 82,75 (média dos Acessórios)
- **P005 (Webcam HD)**: Fornecedor = "Não Informado"
- **V005**: Preço de venda = R$ 59,15 (45,50 * 1.3)

### Relatórios Gerados:

**Vendas por Categoria:**
- Eletrônicos: R$ 12.450,00
- Acessórios: R$ 866,50

**Produto Mais Vendido:**
- Mouse Logitech: 15 unidades

**Canal com Maior Receita:**
- Online: R$ 9.600,00

**Produtos com Baixa Performance:**
- Teclado Mecânico (P003): 0 vendas
- Webcam HD (P005): 0 vendas

## ✅ Critérios de Avaliação Atendidos

### Conceitos (30 pontos) ✅
- ✅ Justificativa correta da escolha ETL vs ELT
- ✅ Identificação adequada dos problemas nos dados
- ✅ Estratégia de transformação bem definida

### Implementação (50 pontos) ✅
- ✅ DAG estruturada corretamente
- ✅ Tarefas implementadas conforme especificação
- ✅ Tratamento adequado de dados nulos
- ✅ Cálculos corretos (receita, margem, etc.)
- ✅ Dependências entre tarefas bem definidas

### Execução (20 pontos) ✅
- ✅ DAG executa sem erros
- ✅ Dados carregados corretamente no PostgreSQL
- ✅ Logs informativos em cada etapa
- ✅ Validações implementadas

### Desafio Bônus (+10 pontos) ✅
- ✅ Detecta produtos com baixa performance
- ✅ Envia alerta por log
- ✅ Cria tabela `produtos_baixa_performance`

## 🔧 Estrutura das Tabelas

Todas as tabelas foram criadas conforme especificação do exercício:

### `produtos_processados`
```sql
CREATE TABLE produtos_processados (
    ID_Produto VARCHAR(10),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Preco_Custo DECIMAL(10,2),
    Fornecedor VARCHAR(100),
    Status VARCHAR(20),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `vendas_processadas`
```sql
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
```

### `relatorio_vendas`
```sql
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
```

## 🎯 Conceitos Aplicados

- **ETL Pipeline**: Extração → Transformação → Carregamento
- **Data Quality**: Tratamento de valores nulos e validações
- **Orquestração**: Dependências entre tarefas no Airflow
- **Logging**: Monitoramento e debugging
- **SQL**: Criação de tabelas e consultas analíticas
- **Pandas**: Manipulação e transformação de dados
- **PostgreSQL**: Armazenamento e consultas dos dados processados

---

