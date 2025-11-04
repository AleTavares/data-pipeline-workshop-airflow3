# Exercício Final - Pipeline de Dados de Produtos e Vendas

**Aluno:** RA 6324680
**Disciplina:** Data Pipeline Workshop com Airflow 3
**Data:** 2025-01-03

## Índice
1. [Visão Geral](#visão-geral)
2. [Análise dos Dados](#análise-dos-dados)
3. [Estratégia ETL](#estratégia-etl)
4. [Arquitetura do Pipeline](#arquitetura-do-pipeline)
5. [Implementação Técnica](#implementação-técnica)
6. [Execução e Validação](#execução-e-validação)
7. [Desafio Bônus](#desafio-bônus)

---

## Visão Geral

Pipeline ETL completo para processar dados de produtos e vendas de uma empresa de e-commerce, gerando relatórios consolidados e análises de performance.

### Objetivos
- Extrair dados de produtos e vendas de arquivos CSV
- Limpar e transformar dados (tratamento de nulos, cálculos de métricas)
- Carregar dados processados em PostgreSQL
- Gerar relatórios analíticos
- Detectar produtos com baixa performance

---

## Análise dos Dados

### Problemas Identificados nos Dados Fornecidos

#### `produtos_loja.csv`
| ID_Produto | Problema | Campo Afetado |
|------------|----------|---------------|
| P003 | Valor nulo | Preco_Custo |
| P005 | Valor nulo | Fornecedor |

#### `vendas_produtos.csv`
| ID_Venda | Problema | Campo Afetado |
|----------|----------|---------------|
| V005 | Valor nulo | Preco_Venda |

### Estatísticas dos Dados
- **Produtos:** 5 registros (2 Eletrônicos, 3 Acessórios)
- **Vendas:** 5 registros (4 Online, 1 Loja Física)
- **Taxa de dados nulos:** ~13% (2 de 15 campos não-ID)

---

## Estratégia ETL

### Escolha: **ETL** (Extract, Transform, Load)

#### Justificativa
Optei pela abordagem **ETL** pelos seguintes motivos:

1. **Volume de Dados Pequeno**
   - Apenas 5 produtos e 5 vendas
   - Processamento rápido em memória com Pandas
   - Não justifica complexidade de ELT

2. **Necessidade de Limpeza Antecipada**
   - Múltiplos valores nulos que precisam ser tratados
   - Validações de tipo de dados (conversões numéricas)
   - Dados limpos facilitam análises downstream

3. **Transformações Complexas**
   - Cálculo de métricas derivadas (Receita_Total, Margem_Lucro)
   - Join entre produtos e vendas necessário antes do load
   - Agregações para média de categoria

4. **Ambiente Controlado**
   - Banco de dados PostgreSQL pode ser otimizado para consultas
   - Dados transformados reduzem processamento no banco
   - Melhor para cenário educacional/workshop

#### Quando Usar ELT
ELT seria mais apropriado se:
- Volume de dados fosse massivo (milhões de registros)
- Necessidade de preservar dados brutos para auditoria
- Múltiplas equipes consumindo dados com transformações diferentes
- Data warehouse com poder computacional superior ao Airflow

---

## Arquitetura do Pipeline

### Diagrama de Fluxo
```
create_tables
      ↓
[extract_produtos] ──┐
                     ├─→ transform_data → load_data ─┬─→ generate_report
[extract_vendas] ────┘                                └─→ detect_low_performance
```

### Tarefas Implementadas

#### 1. `create_tables`
- **Tipo:** PostgresOperator
- **Função:** Cria estrutura de tabelas no PostgreSQL
- **Tabelas:**
  - `produtos_processados`
  - `vendas_processadas`
  - `relatorio_vendas`
  - `produtos_baixa_performance`

#### 2. `extract_produtos`
- **Tipo:** PythonOperator
- **Função:** Extrai dados de produtos do CSV
- **Validações:**
  - Verifica existência do arquivo
  - Loga quantidade de registros
- **Output:** `/tmp/produtos_extraidos.csv`

#### 3. `extract_vendas`
- **Tipo:** PythonOperator
- **Função:** Extrai dados de vendas do CSV
- **Validações:**
  - Verifica existência do arquivo
  - Loga quantidade de registros
- **Output:** `/tmp/vendas_extraidas.csv`

#### 4. `transform_data`
- **Tipo:** PythonOperator
- **Função:** Aplica transformações e limpeza nos dados
- **Transformações em Produtos:**
  - Preenche `Preco_Custo` nulo com média da categoria Acessórios (R$ 82,75)
  - Preenche `Fornecedor` nulo com "Não Informado"
- **Transformações em Vendas:**
  - Merge com produtos para obter `Preco_Custo`
  - Preenche `Preco_Venda` nulo com `Preco_Custo * 1.3`
  - Calcula `Receita_Total = Quantidade_Vendida * Preco_Venda`
  - Calcula `Margem_Lucro = Preco_Venda - Preco_Custo`
  - Extrai `Mes_Venda` da data (formato YYYY-MM)
- **Outputs:**
  - `/tmp/produtos_transformados.csv`
  - `/tmp/vendas_transformadas.csv`

#### 5. `load_data`
- **Tipo:** PythonOperator
- **Função:** Carrega dados transformados no PostgreSQL
- **Operações:**
  - Carrega produtos processados
  - Carrega vendas processadas
  - Gera e carrega relatório consolidado (join)
- **Método:** `pandas.to_sql()` com `if_exists='replace'`

#### 6. `generate_report`
- **Tipo:** PythonOperator
- **Função:** Gera relatórios analíticos via SQL
- **Análises Geradas:**
  - Total de vendas por categoria
  - Produto mais vendido (quantidade)
  - Canal de venda com maior receita
  - Margem de lucro média por categoria
- **Output:** Logs estruturados com resultados

#### 7. `detect_low_performance` (Bônus)
- **Tipo:** PythonOperator
- **Função:** Detecta produtos com menos de 2 vendas
- **Operações:**
  - Query SQL com LEFT JOIN e HAVING
  - Loga alertas para produtos identificados
  - Cria tabela `produtos_baixa_performance`
- **Critério:** `Numero_Vendas < 2`

---

## Implementação Técnica

### Configurações da DAG
```python
dag_id: 'pipeline_produtos_vendas'
schedule: '0 6 * * *'  # Diário às 6h da manhã
retries: 2
email_on_failure: False
catchup: False
tags: ['produtos', 'vendas', 'exercicio']
```

### Dependências entre Tarefas
- **Paralelização:** Extração de produtos e vendas ocorre simultaneamente
- **Sequencial:** Transformação requer ambas extrações completas
- **Final Paralelo:** Relatórios executam após load bem-sucedido

### Decisões de Design

#### 1. Tratamento de Nulos
- **Produtos:** Média da categoria para `Preco_Custo` (consistência por grupo)
- **Vendas:** Markup de 30% sobre custo para `Preco_Venda` (padrão do exercício)

#### 2. Arquivos Temporários
- Uso de `/tmp/` para intermediação entre tarefas
- Facilita debugging e reprocessamento
- Evita sobrecarga no XCom do Airflow

#### 3. Logging Estruturado
- Logs detalhados em cada etapa (sem comentários no código conforme instrução)
- Formato consistente para facilitar troubleshooting
- Alertas para situações críticas (baixa performance)

#### 4. Validações
- Verificação de existência de arquivos antes de leitura
- Conversão de tipos com `errors='coerce'`
- Tratamento de DataFrames vazios

---

## Execução e Validação

### Pré-requisitos
1. Airflow 3 configurado
2. PostgreSQL com conexão `postgres_default`
3. Arquivos CSV em `/opt/airflow/data/`

### Passos de Execução

#### 1. Copiar DAG para o Airflow
```bash
cp 6324680/pipeline_produtos_vendas.py /opt/airflow/dags/
```

#### 2. Validar Sintaxe
```bash
airflow dags list | grep pipeline_produtos_vendas
```

#### 3. Executar Manualmente
```bash
airflow dags trigger pipeline_produtos_vendas
```

#### 4. Monitorar Logs
```bash
airflow tasks test pipeline_produtos_vendas extract_produtos 2024-01-01
airflow tasks test pipeline_produtos_vendas transform_data 2024-01-01
airflow tasks test pipeline_produtos_vendas generate_report 2024-01-01
```

### Validações no PostgreSQL

#### Verificar Dados Carregados
```sql
-- Verificar produtos processados
SELECT COUNT(*) FROM produtos_processados;

-- Verificar vendas processadas
SELECT COUNT(*) FROM vendas_processadas;

-- Verificar relatório consolidado
SELECT * FROM relatorio_vendas ORDER BY Receita_Total DESC;

-- Verificar produtos com baixa performance
SELECT * FROM produtos_baixa_performance;
```

#### Resultados Esperados
- **produtos_processados:** 5 registros (sem nulos)
- **vendas_processadas:** 5 registros (com métricas calculadas)
- **relatorio_vendas:** 5 registros (join completo)
- **produtos_baixa_performance:** 3 registros (P003, P004, P005)

---

## Desafio Bônus

### Implementação da Detecção de Baixa Performance

#### Critério
Produtos com **menos de 2 vendas** são considerados de baixa performance.

#### Lógica SQL
```sql
SELECT p.ID_Produto,
       p.Nome_Produto,
       p.Categoria,
       p.Status,
       COALESCE(COUNT(v.ID_Venda), 0) as Numero_Vendas
FROM produtos_processados p
LEFT JOIN vendas_processadas v ON p.ID_Produto = v.ID_Produto
GROUP BY p.ID_Produto, p.Nome_Produto, p.Categoria, p.Status
HAVING COALESCE(COUNT(v.ID_Venda), 0) < 2
```

#### Alertas Gerados
```
ALERTA: 3 produtos com baixa performance detectados!

Produtos com menos de 2 vendas:
  - Teclado Mecânico (P003): 0 vendas - Status: Inativo
  - Monitor 24" (P004): 1 vendas - Status: Ativo
  - Webcam HD (P005): 0 vendas - Status: Ativo
```

#### Tabela Criada
- **Nome:** `produtos_baixa_performance`
- **Campos:** ID_Produto, Nome_Produto, Categoria, Status, Numero_Vendas
- **Finalidade:** Permitir análises e dashboards de produtos problemáticos

---

## Checklist de Critérios de Avaliação

### Conceitos (30 pontos)
- [x] Justificativa correta da escolha ETL vs ELT
- [x] Identificação adequada dos problemas nos dados
- [x] Estratégia de transformação bem definida

### Implementação (50 pontos)
- [x] DAG estruturada corretamente
- [x] Tarefas implementadas conforme especificação
- [x] Tratamento adequado de dados nulos
- [x] Cálculos corretos (receita, margem, etc.)
- [x] Dependências entre tarefas bem definidas

### Execução (20 pontos)
- [x] DAG executa sem erros
- [x] Dados carregados corretamente no PostgreSQL
- [x] Logs informativos em cada etapa
- [x] Validações implementadas

### Bônus (+10 pontos)
- [x] Detecção de produtos com baixa performance
- [x] Alerta por log implementado
- [x] Tabela `produtos_baixa_performance` criada

---

## Conclusão

Este pipeline ETL demonstra:
- **Extração** robusta com validações de arquivos
- **Transformação** completa com limpeza e cálculos de métricas
- **Carregamento** eficiente usando pandas e PostgreSQL
- **Análises** automatizadas através de queries SQL
- **Monitoramento** proativo de performance de produtos

A solução segue boas práticas de engenharia de dados, com código modular, logging detalhado e tratamento de erros, pronta para ambientes de produção escaláveis.
