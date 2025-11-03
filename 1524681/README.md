# Pipeline de Dados - Produtos e Vendas
**Aluno:** David Gilmour Souza
**RA:** 1524681

## 📋 Descrição da Solução

Este pipeline ETL processa dados de produtos e vendas de uma empresa de e-commerce, aplicando transformações, limpeza de dados e gerando relatórios analíticos.

## 🏗️ Arquitetura

### Escolha: ETL (Extract, Transform, Load)

Optei pela abordagem ETL pelos seguintes motivos:
1. **Volume de dados moderado** - permite processamento em memória
2. **Transformações complexas** - melhor executadas antes do carregamento
3. **Qualidade dos dados** - validação e limpeza antes da persistência
4. **Performance do banco** - evita sobrecarga com transformações no PostgreSQL

## 🔄 Fluxo do Pipeline

```
extract_produtos ─┐
                  ├─> transform_data -> create_tables -> load_data -> generate_report -> detect_low_performance
extract_vendas ───┘
```

## 📊 Transformações Aplicadas

### Limpeza de Dados
- **Preco_Custo nulo:** Preenchido com média da categoria
- **Fornecedor nulo:** Preenchido com "Não Informado"
- **Preco_Venda nulo:** Calculado como Preco_Custo * 1.3 (margem de 30%)

### Cálculos
- **Receita_Total:** Quantidade_Vendida × Preco_Venda
- **Margem_Lucro:** Preco_Venda - Preco_Custo
- **Mes_Venda:** Extraído de Data_Venda (formato YYYY-MM)

## 📈 Relatórios Gerados

1. **Vendas por Categoria**
   - Total de quantidade vendida
   - Receita total
   - Margem média

2. **Produto Mais Vendido**
   - Nome do produto
   - Quantidade total vendida
   - Receita gerada

3. **Análise por Canal de Venda**
   - Número de vendas
   - Quantidade total
   - Receita por canal

4. **Margem de Lucro**
   - Margem média, mínima e máxima por categoria

## 🎁 Desafio Bônus

Implementei detecção de produtos com baixa performance:
- Identifica produtos com menos de 2 vendas
- Gera alertas via log
- Cria tabela `produtos_baixa_performance`

## 🚀 Como Executar

1. Copiar arquivos CSV para `/opt/airflow/dags/data/`
2. Configurar conexão PostgreSQL no Airflow
3. Ativar a DAG no Airflow UI
4. Executar manualmente ou aguardar schedule (6h da manhã)

## ✅ Validações Implementadas

- Verificação de existência de arquivos
- Contagem de registros em cada etapa
- Logs detalhados para debugging
- Validação de inserção no banco

## 📝 Observações

- Todos os requisitos do exercício foram atendidos
- Código comentado para facilitar manutenção
- Tratamento de erros em todas as funções
- Logs informativos para monitoramento
