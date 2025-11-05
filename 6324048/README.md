1 – Problemas Encontrados

Nos arquivos produtos_loja.csv e vendas_produtos.csv havia dados faltando que precisavam ser tratados antes de qualquer análise ou carregamento no banco.

No produtos_loja.csv, o Preço de Custo do produto P003 (Teclado Mecânico) estava vazio, e o Fornecedor do produto P005 (Webcam HD) não tinha informação. Esses campos são importantes para calcular margens e custos corretamente.

No vendas_produtos.csv, o Preço de Venda da venda V005 (Produto P002) estava ausente, o que poderia gerar problemas ao calcular Receita Total e Margem de Lucro.

2 – Como Tratamos os Dados Faltantes

Adotamos três soluções para preencher os valores nulos:

Preço de Custo: substituído pela média da categoria do produto. Assim, o custo do Teclado Mecânico ausente foi igual à média dos outros produtos de “Acessórios”.

Fornecedor: campos vazios foram preenchidos com “Não Informado”.

Preço de Venda: calculado como Preço de Custo × 1,3, aplicando uma margem padrão de 30%.

Tudo isso foi feito dentro da função transform_data() usando Pandas.

3 – Estratégia ETL

O projeto segue o modelo ETL (Extrair, Transformar, Carregar):

Extração: os dados são lidos dos arquivos CSV.

Transformação: os dados são tratados e calculadas novas colunas (Receita Total, Margem de Lucro, Mês da Venda).

Carga: os dados já limpos são inseridos no PostgreSQL.

Escolhemos ETL porque os arquivos são pequenos e o processamento direto no Python é mais rápido e controlável.

4 – Estrutura da DAG

A DAG se chama pipeline_produtos_vendas e tem 7 tarefas principais:

extract_produtos: lê produtos_loja.csv e conta os registros.

extract_vendas: lê vendas_produtos.csv.

transform_data: aplica transformações, calcula métricas e trata dados ausentes.

create_tables: cria as tabelas no PostgreSQL.

load_data: insere os dados transformados nas tabelas.

generate_report: cria relatórios de vendas por categoria, produto mais vendido, canal com maior receita e margem média por categoria.

baixa_performance (opcional): identifica produtos com menos de 2 vendas e grava na tabela produtos_baixa_performance.

Fluxo de execução:

extract_produtos ┐
                 ├──> transform_data → create_tables → load_data → generate_report → baixa_performance
extract_vendas ┘

5 – Configurações da DAG

Nome: pipeline_produtos_vendas

Agendamento: diariamente às 6h (0 6 * * *)

Tentativas de retry: 2

E-mail em falha: desativado

Tags: ['produtos', 'vendas', 'exercicio']

Data de início: 01/01/2025

A DAG roda automaticamente todos os dias, com logs detalhados e tolerância a falhas.

6 – Relatórios e Métricas

Depois de rodar o pipeline, a DAG gera um relatório com:

Total de vendas por categoria

Produto mais vendido

Canal com maior receita

Margem média por categoria

Exemplo de saída:

{
  "Total por Categoria": {"Eletrônicos": 10550.0, "Acessórios": 2700.0},
  "Produto mais vendido": "Mouse Logitech",
  "Canal com maior receita": "Online",
  "Margem média por Categoria": {"Eletrônicos": 600.0, "Acessórios": 75.5}
}

7 – Produtos de Baixa Performance

A tarefa extra baixa_performance identifica produtos com poucas vendas (menos de 2 unidades) e grava essas informações em produtos_baixa_performance.

Além disso, ela imprime um alerta no log do Airflow para que seja fácil identificar produtos que vendem pouco.