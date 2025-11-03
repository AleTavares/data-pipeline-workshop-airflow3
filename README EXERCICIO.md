# Pipeline de Produtos e Vendas - Exercício Final

## 📋 Descrição da Solução

Este projeto implementa um pipeline ETL completo para processar dados de produtos e vendas, conforme especificado no exercício final do workshop de Apache Airflow.

## 🎯 Objetivos Atendidos

### ✅ Parte 1: Análise e Planejamento

**Problemas Identificados nos Dados:**
- `Preco_Custo` nulo para produto P003 (Teclado Mecânico)
- `Fornecedor` vazio para produto P005 (Webcam HD)  
- `Preco_Venda` nulo para venda V005 (Mouse Logitech)

**Estratégia ETL Escolhida:**
- **ETL** (Extract, Transform, Load) foi escolhida porque:
  - Volume de dados pequeno e bem estruturado
  - Transformações bem definidas e simples
  - Processamento pode ser feito em memória com pandas
  - Melhor controle de qualidade antes do carregamento

### ✅ Parte 2: Implementação da DAG

A DAG `pipeline_produtos_vendas` foi implementada com todas as 6 tarefas solicitadas:

#### 🔍 Task 1: `extract_produtos`
- ✅ Lê arquivo `produtos_loja.csv`
- ✅ Valida existência do arquivo
- ✅ Log detalhado do número de registros extraídos
- ✅ Log adicional com estatísticas por categoria

#### 🔍 Task 2: `extract_vendas`
- ✅ Lê arquivo `vendas_produtos.csv`
- ✅ Valida existência do arquivo
- ✅ Log detalhado do número de registros extraídos
- ✅ Log adicional com estatísticas por canal

#### 🔄 Task 3: `transform_data`
**Limpeza de Dados:**
- ✅ Preenche `Preco_Custo` nulo com média da categoria
- ✅ Preenche `Fornecedor` nulo com "Não Informado"
- ✅ Preenche `Preco_Venda` nulo com `Preco_Custo * 1.3`

**Transformações:**
- ✅ Calcula `Receita_Total` = `Quantidade_Vendida * Preco_Venda`
- ✅ Calcula `Margem_Lucro` = `Preco_Venda - Preco_Custo`
- ✅ Cria campo `Mes_Venda` extraído de `Data_Venda`

#### 🗃️ Task 4: `create_tables`
- ✅ Cria tabela `produtos_processados`
- ✅ Cria tabela `vendas_processadas`
- ✅ Cria tabela `relatorio_vendas`
- ✅ Implementa DROP TABLE IF EXISTS para segurança

#### 📥 Task 5: `load_data`
- ✅ Carrega dados transformados nas tabelas PostgreSQL
- ✅ Cria relatório consolidado com JOIN das tabelas
- ✅ Valida se dados foram inseridos corretamente
- ✅ Log detalhado de contadores de registros

#### 📊 Task 6: `generate_report`
- ✅ Total de vendas por categoria
- ✅ Produto mais vendido
- ✅ Canal de venda com maior receita
- ✅ Margem de lucro média por categoria
- ✅ Resumo geral com estatísticas consolidadas

### ✅ Parte 3: Configuração e Execução

**Configurações da DAG:**
- ✅ Schedule: diário às 6h da manhã (`'0 6 * * *'`)
- ✅ Retry: 2 tentativas
- ✅ Email on failure: False
- ✅ Tags: `['produtos', 'vendas', 'exercicio']`

## 🏗️ Arquitetura da Solução

```
extract_produtos ──┐
                   ├── transform_data → create_tables → load_data → generate_report
extract_vendas ────┘
```

**Dependências:**
1. `extract_produtos` e `extract_vendas` executam em paralelo
2. `transform_data` aguarda ambas as extrações
3. `create_tables` executa após transformação
4. `load_data` carrega dados nas tabelas criadas
5. `generate_report` gera relatórios finais

## 📊 Estrutura das Tabelas

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

## 🚀 Como Executar

### Pré-requisitos
- Apache Airflow configurado
- PostgreSQL em execução
- Conexão `postgres_default` configurada no Airflow
- Arquivos CSV na pasta `/opt/airflow/data/`

### Execução
1. Copie o arquivo `pipeline_produtos_vendas.py` para a pasta `dags/`
2. A DAG aparecerá automaticamente na interface do Airflow
3. Execute manualmente ou aguarde o schedule às 6h da manhã
4. Monitore os logs de cada tarefa

## 📈 Relatórios Gerados

O pipeline gera automaticamente:

1. **Vendas por Categoria**: Total de receita e quantidade por categoria
2. **Produto Mais Vendido**: Produto com maior quantidade vendida
3. **Canal com Maior Receita**: Canal de venda mais lucrativo
4. **Margem por Categoria**: Margem de lucro média por categoria
5. **Resumo Geral**: Estatísticas consolidadas

## 🔧 Recursos Técnicos Utilizados

- **Apache Airflow**: Orquestração do pipeline
- **Pandas**: Manipulação e transformação de dados
- **PostgreSQL**: Armazenamento dos dados processados
- **Python**: Linguagem de programação principal
- **CSV**: Formato dos dados de entrada

## 📝 Logs e Monitoramento

Cada tarefa gera logs detalhados incluindo:
- Número de registros processados
- Estatísticas dos dados
- Validações de qualidade
- Relatórios consolidados
- Tratamento de erros

## ✨ Diferenciais Implementados

- **Validação de Arquivos**: Verifica existência antes de processar
- **Logs Detalhados**: Informações completas sobre cada etapa
- **Tratamento de Nulos**: Estratégia inteligente para cada tipo de dado
- **Validação de Dados**: Confirma integridade após carregamento
- **Relatórios Automáticos**: Estatísticas completas nos logs
- **Estrutura Robusta**: Tratamento de erros e retry configurado

---

**Desenvolvido para o Workshop de Apache Airflow - Exercício Final**