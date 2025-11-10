# Pipeline de Produtos e Vendas - Exercício Final

## 📋 Perguntas e Respostas sobre a Implementação

### **Parte 1: Análise e Planejamento**

#### **Q1: Quais problemas foram identificados nos dados?**

**R:** Os dados apresentam os seguintes problemas:

- **Produtos:** Campo `Preco_Custo` nulo (P003) e `Fornecedor` nulo (P005)
- **Vendas:** Campo `Preco_Venda` nulo (V005)

#### **Q2: Qual estratégia ETL foi definida e por quê?**

**R:** Foi escolhida a abordagem **ETL** porque:

- Dados estruturados e de pequeno volume
- Transformações simples (limpeza e cálculos)
- Melhor controle de qualidade antes do carregamento
- Menor uso de recursos no PostgreSQL de destino

### **Parte 2: Implementação das Tasks**

#### **Q3: Como foi implementada a Task `extract_produtos`?**

**R:** A task extrai dados do arquivo `produtos_loja.csv`:

```python
def extract_produtos():
    # Lê CSV, valida existência do arquivo
    # Retorna DataFrame via XCom
    # Log do número de registros extraídos
```

#### **Q4: Como foi implementada a Task `extract_vendas`?**

**R:** Similar à extração de produtos, mas para `vendas_produtos.csv`:

```python
def extract_vendas():
    # Lê CSV de vendas, valida arquivo
    # Retorna DataFrame via XCom
    # Log do número de registros extraídos
```

#### **Q5: Quais transformações foram aplicadas na Task `transform_data`?**

**R:** As seguintes transformações de limpeza e enriquecimento:

**Limpeza de dados:**

- `Preco_Custo` nulo → preenchido com média da categoria
- `Fornecedor` nulo → preenchido com "Não Informado"
- `Preco_Venda` nulo → calculado como `Preco_Custo * 1.3`

**Cálculos derivados:**

- `Receita_Total` = `Quantidade_Vendida * Preco_Venda`
- `Margem_Lucro` = `Preco_Venda - Preco_Custo`
- `Mes_Venda` = extraído de `Data_Venda` (formato YYYY-MM)

#### **Q6: Como foi implementada a Task `create_tables`?**

**R:** Utiliza `PostgresOperator` para criar 4 tabelas:

- `produtos_processados` - dados de produtos limpos
- `vendas_processadas` - dados de vendas com cálculos
- `relatorio_vendas` - join consolidado
- `produtos_baixa_performance` - para o desafio bônus

#### **Q7: Como funciona a Task `load_data`?**

**R:** Carrega os dados transformados no PostgreSQL:

- Recebe DataFrames via XCom das tasks anteriores
- Usa `PostgresHook` para conexão
- Insere dados nas tabelas com validação de sucesso
- Gera logs informativos do processo

#### **Q8: O que gera a Task `generate_report`?**

**R:** Produz relatório analítico com:

- Total de vendas por categoria
- Produto mais vendido
- Canal de venda com maior receita
- Margem de lucro média por categoria

### **Parte 3: Configuração da DAG**

#### **Q9: Como foi configurada a DAG?**

**R:** Configuração conforme especificação:

```python
dag_id='pipeline_produtos_vendas'
schedule='0 6 * * *'  # Diário às 6h
retries=2
email_on_failure=False
tags=['produtos', 'vendas', 'exercicio', 'bonus']
```

#### **Q10: Como estão definidas as dependências entre tasks?**

**R:** Fluxo de dependências:

```
[extract_produtos, extract_vendas] >> transform_data
create_tables >> load_data
transform_data >> load_data
load_data >> [generate_report, analyze_performance]
```

### **Desafio Bônus**

#### **Q11: Como foi implementado o desafio bônus?**

**R:** A task `analyze_performance` implementa:

**Funcionalidade:**

- Detecta produtos com menos de 2 vendas totais
- Usa CTE para agregar vendas por produto
- Gera alertas via log quando encontra baixa performance
- Carrega resultados na tabela `produtos_baixa_performance`

**Query SQL:**

```sql
WITH VendasAgregadas AS (
    SELECT nome_produto, categoria, SUM(quantidade_vendida) AS total_vendido
    FROM relatorio_vendas
    GROUP BY nome_produto, categoria
)
SELECT nome_produto, categoria, total_vendido,
       'BAIXA PERFORMANCE (< 2)' AS status_alerta
FROM VendasAgregadas
WHERE total_vendido < 2;
```

### **Estrutura do Código**

#### **Q12: Quais são os componentes principais do arquivo?**

**R:** O arquivo contém:

- **Imports:** Airflow, PostgreSQL, logging
- **Configurações:** default_args, conexões, arquivos
- **Funções:** extract_produtos, extract_vendas, transform_data, load_data, generate_report, analyze_performance
- **DAG:** Definição e orquestração das tasks

#### **Q13: Como é feito o tratamento de erros?**

**R:** Implementado através de:

- Retry automático (2 tentativas)
- Delay entre tentativas (5 minutos)
- Logs informativos em cada etapa
- Validações de existência de arquivos
- Verificação de dados carregados

### **Execução e Validação**

#### **Q14: Como executar e validar o pipeline?**

**R:** Passos para execução:

1. Colocar arquivos CSV na pasta `/opt/airflow/data/`
2. Ativar a DAG no Airflow UI
3. Executar manualmente ou aguardar schedule
4. Verificar logs de cada task
5. Validar dados no PostgreSQL:

```sql
SELECT COUNT(*) FROM produtos_processados;
SELECT COUNT(*) FROM vendas_processadas;
SELECT COUNT(*) FROM relatorio_vendas;
```

#### **Q15: Quais são os critérios de sucesso?**

**R:** Pipeline bem-sucedido quando:

- Todas as tasks executam sem erro
- Dados são carregados nas 4 tabelas
- Relatório é gerado com métricas corretas
- Logs mostram informações detalhadas
- Desafio bônus identifica produtos de baixa performance

---

## 🚀 Como Executar

1. **Preparar ambiente:**

   ```bash
   docker compose up -d
   ```

2. **Acessar Airflow:**

   - URL: http://localhost:8080
   - User: admin / Password: [airflow_token]

3. **Ativar DAG:**

   - Localizar `pipeline_produtos_vendas`
   - Ativar toggle
   - Executar manualmente

4. **Verificar resultados:**
   - Logs no Airflow UI
   - Dados no PostgreSQL (porta 2001)

## 📊 Resultados Esperados

- **produtos_processados:** 5 registros limpos
- **vendas_processadas:** 5 registros com cálculos
- **relatorio_vendas:** 5 registros consolidados
- **produtos_baixa_performance:** Produtos com < 2 vendas
