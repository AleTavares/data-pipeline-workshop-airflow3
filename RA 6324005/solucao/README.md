# Pipeline de Produtos e Vendas - Solução Completa

**RA:** 6324005  
**nome:** Alison Medeiros


Esta solução implementa um pipeline ETL completo usando Apache Airflow 3 para processar dados de produtos e vendas, incluindo transformações de dados, tratamento de valores nulos, geração de relatórios e detecção de produtos com baixa performance.


1. Extrair dados de produtos e vendas de arquivos CSV
2. Transformar e limpar dados (tratamento de valores nulos)
3. Carregar dados processados em tabelas PostgreSQL
4. Gerar relatórios de vendas
5. Detectar produtos com baixa performance

 Estrutura da Solução

```
RA 6324005/
└── solucao/
    ├── pipeline_produtos_vendas.py  # DAG principal
    ├── README.md                     # Esta documentação
    ├── ANALISE_DADOS.md             # Análise de qualidade dos dados
    └── data/                         # Arquivos CSV de entrada
        ├── produtos_loja.csv
        └── vendas_produtos.csv
```
 Arquitetura da DAG

 Tarefas Implementadas

1. **extract_produtos**: Extrai dados do arquivo `produtos_loja.csv`
2. **extract_vendas**: Extrai dados do arquivo `vendas_produtos.csv`
3. **transform_data**: Transforma e limpa os dados
4. **create_tables**: Cria tabelas no PostgreSQL
5. **load_data**: Carrega dados transformados nas tabelas
6. **generate_report**: Gera relatório de métricas de vendas
7. **detect_low_performance**: Detecta produtos com baixa performance

Fluxo de Execução

```
[extract_produtos, extract_vendas] 
    ↓
transform_data
    ↓
create_tables → load_data → [generate_report, detect_low_performance]
```

Transformações Implementadas

 Limpeza de Dados

1. **Produtos:**
   - Preenche `Preco_Custo` nulo com média da categoria
   - Preenche `Fornecedor` nulo com "Não Informado"

2. **Vendas:**
   - Preenche `Preco_Venda` nulo com `Preco_Custo * 1.3`

Transformações

- Calcula `Receita_Total = Quantidade_Vendida * Preco_Venda`
- Calcula `Margem_Lucro = Preco_Venda - Preco_Custo`
- Extrai `Mes_Venda` da data de venda
 Estrutura das Tabelas

 produtos_processados
- ID_Produto, Nome_Produto, Categoria
- Preco_Custo, Fornecedor, Status
- Data_Processamento (TIMESTAMP)

 vendas_processadas
- ID_Venda, ID_Produto, Quantidade_Vendida
- Preco_Venda, Data_Venda, Canal_Venda
- Receita_Total, Mes_Venda
- Data_Processamento (TIMESTAMP)

 relatorio_vendas
- ID_Venda, Nome_Produto, Categoria
- Quantidade_Vendida, Receita_Total, Margem_Lucro
- Canal_Venda, Mes_Venda
- Data_Processamento (TIMESTAMP)

 produtos_baixa_performance
- ID_Produto, Nome_Produto, Categoria
- Total_Vendas, Receita_Total, Status
- Data_Processamento (TIMESTAMP)

Relatórios Gerados

A task `generate_report` gera os seguintes relatórios:

1. **Total de vendas por categoria**
2. **Produto mais vendido** (por quantidade)
3. **Canal de venda com maior receita**
4. **Margem de lucro média por categoria**

 Detecção de Baixa Performance

A task `detect_low_performance` identifica produtos com menos de 2 vendas e:

- Registra alertas nos logs com informações detalhadas
- Cria tabela `produtos_baixa_performance` com os produtos detectados
- Inclui produtos sem vendas (0 vendas)

 Exemplo de Alerta

```
  ALERTA: PRODUTOS COM BAIXA PERFORMANCE DETECTADOS!
Total de produtos com menos de 2 vendas: 2

  Produto: Teclado Mecânico (ID: P003) | Categoria: Acessórios | 
    Vendas: 0 | Receita: R$ 0.00 | Status: Inativo
```

 Configuração da DAG

- **Schedule**: Diário às 6h da manhã (`0 6 * * *`)
- **Retries**: 2 tentativas
- **Retry Delay**: 5 minutos
- **Tags**: `['produtos', 'vendas', 'exercicio']`
- **Email on failure**: False

 Dependências

As seguintes bibliotecas são necessárias (já no `requirements.txt` do projeto):

- `pandas==2.1.4`
- `apache-airflow-providers-postgres==5.7.1`
- `psycopg2-binary==2.9.9`
- `sqlalchemy==1.4.53`

 Validações Implementadas
1. **Validação de arquivos**: Verifica existência dos CSVs antes de processar
2. **Validação de dados**: Confirma quantidade de registros inseridos
3. **Tratamento de erros**: Logs detalhados para debugging
4. **Validação de qualidade**: Detecção de produtos com baixa performance

Análise de Qualidade dos Dados

Consulte o arquivo `ANALISE_DADOS.md` para detalhes sobre:
- Campos com valores nulos identificados
- Estratégias de tratamento implementadas
- Recomendações de melhorias

 Observações Técnicas

1. **Conexão PostgreSQL**: Configurada via variável de ambiente `AIRFLOW__CONN__POSTGRES_DEFAULT`
2. **Timestamps**: Campo `Data_Processamento` preenchido automaticamente pelo PostgreSQL
3. **Paralelização**: Tasks `generate_report` e `detect_low_performance` executam em paralelo
4. **Tratamento de nulos**: Estratégias inteligentes (média por categoria, valores derivados)

 Execução

Para executar a DAG:

1. Certifique-se de que o Airflow está rodando
2. Acesse a interface do Airflow (http://localhost:5000)
3. Localize a DAG `pipeline_produtos_vendas`
4. Execute manualmente ou aguarde o schedule diário

 Validação

Após a execução, valide os dados no PostgreSQL:

```sql
-- Verificar produtos processados
SELECT * FROM produtos_processados;

-- Verificar vendas processadas
SELECT * FROM vendas_processadas;

-- Verificar relatório de vendas
SELECT * FROM relatorio_vendas;

-- Verificar produtos com baixa performance
SELECT * FROM produtos_baixa_performance;
```

    Arquivos da Solução

- **pipeline_produtos_vendas.py**: DAG completa com todas as tarefas
- **README.md**: Esta documentação
- **ANALISE_DADOS.md**: Análise detalhada dos problemas de qualidade dos dados
- **data/produtos_loja.csv**: Dados de produtos
- **data/vendas_produtos.csv**: Dados de vendas

---

**Desenvolvido por:** RA 6324005  
**Ferramenta:** Apache Airflow 3  
**Banco de Dados:** PostgreSQL 14

