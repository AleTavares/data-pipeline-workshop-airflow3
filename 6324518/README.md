# Exercício Final - Pipeline de Dados de Produtos
**RA:** 6324518

---

## Resumo da Implementação

Este projeto implementa um pipeline ETL completo para processar dados de produtos e vendas, seguindo todas as especificações do exercício final.

## Arquitetura da Solução

### 1. Estratégia ETL vs ELT
**Escolha:** ETL (Extract, Transform, Load)

**Justificativa:**
- Os dados são relativamente pequenos e estruturados;
- As transformações são bem definidas e não muito complexas;
- O PostgreSQL será usado principalmente para consultas e relatórios;
- A limpeza e transformação dos dados antes do carregamento garante qualidade dos dados.

### 2. Problemas Identificados nos Dados

1. **produtos_loja.csv:**
   - `Preco_Custo` com valores nulos (P003)
   - `Fornecedor` com valores nulos (P005)

2. **vendas_produtos.csv:**
   - `Preco_Venda` com valores nulos (V005)

### 3. Transformações Aplicadas

1. **Limpeza:**
   - Preenchimento de `Preco_Custo` nulo com média da categoria
   - Preenchimento de `Fornecedor` nulo com "Não Informado"
   - Preenchimento de `Preco_Venda` nulo com `Preco_Custo * 1.3`

2. **Cálculos:**
   - `Receita_Total = Quantidade_Vendida * Preco_Venda`
   - `Margem_Lucro = Preco_Venda - Preco_Custo`
   - `Mes_Venda` extraído de `Data_Venda`

---

## Estrutura da DAG

### 1. Configuração
- **Nome:** `pipeline_produtos_vendas`
- **Schedule:** Diário às 6h da manhã (`0 6 * * *`)
- **Retries:** 2 tentativas
- **Tags:** ['produtos', 'vendas', 'exercicio']

### 2. Tarefas Implementadas

1. **create_tables:** Cria todas as tabelas necessárias no PostgreSQL
2. **extract_produtos:** Extrai dados de `produtos_loja.csv`
3. **extract_vendas:** Extrai dados de `vendas_produtos.csv`
4. **transform_data:** Aplica limpeza e transformações nos dados
5. **load_data:** Carrega dados transformados no PostgreSQL
6. **generate_report:** Gera relatórios de negócio
7. **detect_low_performance_products:** (Bônus) Detecta produtos com baixa performance

### 3. Dependências
```
create_tables >> [extract_produtos, extract_vendas] >> transform_data >> load_data >> generate_report >> detect_low_performance_products
```

---

## Tabelas Criadas

### produtos_processados
Armazena dados limpos e processados dos produtos.

### vendas_processadas
Armazena dados de vendas com cálculos de receita e margem.

### relatorio_vendas
Join entre produtos e vendas para análises consolidadas.

### produtos_baixa_performance (Bônus)
Produtos com menos de 2 vendas totais.

## Relatórios Gerados

1. **Total de vendas por categoria**
2. **Produto mais vendido**
3. **Canal de venda com maior receita**
4. **Margem de lucro média por categoria**

---

## Desafio Bônus

Implementado sistema de detecção de produtos com baixa performance:
- Identifica produtos com menos de 2 vendas
- Gera alertas nos logs
- Cria tabela específica para análise

---

# Detalhes

## Como Executar

1. Certifique-se de que o Airflow está rodando na porta 5000
2. A DAG `pipeline_produtos_vendas` deve aparecer na interface
3. Execute manualmente ou aguarde o schedule diário
4. Monitore os logs de cada tarefa
5. Verifique os dados nas tabelas PostgreSQL

## Validações Implementadas

- Verificação de existência dos arquivos CSV
- Contagem de registros em cada etapa
- Validação de inserção no banco de dados
- Logs informativos em todas as etapas

## Tecnologias Utilizadas

- **Apache Airflow** - Orquestração do pipeline
- **PostgreSQL** - Banco de dados de destino
- **Pandas** - Manipulação e transformação de dados
- **Python** - Linguagem principal

## Observações

- Todos os dados temporários são salvos em `/tmp/` durante o processamento
- A DAG está configurada para não executar catch-up de datas passadas
- Os logs fornecem informações detalhadas sobre cada etapa do processo