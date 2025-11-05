1 - Identificação dos Problemas

- Arquivos produtos_loja.csv e vendas_produtos.csv possuíam valores nulos que precisavam ser tratados.

produtos_loja.csv:

Preco_Custo nulo no produto P003 (Teclado Mecânico).

Fornecedor nulo no produto P005 (Webcam HD).

vendas_produtos.csv:

Preco_Venda nulo na venda V005 (Produto P002).

Valores ausentes poderiam gerar erros em cálculos de margens, custos e comprometer a consistência dos dados.

2 - Estratégia de Tratamento dos Dados Nulos

Preco_Custo (produtos_loja.csv): preenchido com a média da categoria correspondente.

Fornecedor (produtos_loja.csv): substituído por "Não Informado".

Preco_Venda (vendas_produtos.csv): calculado como Preco_Custo * 1.3 (margem de 30%).

Transformações implementadas na função transform_data() usando Pandas.

3 - Estratégia ETL ou ELT

Estrutura adotada: ETL (Extract, Transform, Load).

Motivos:

Pequeno volume de dados (arquivos CSV), processados localmente.

Limpeza e transformação realizadas no Python antes do carregamento.

PostgreSQL usado apenas como repositório final.

4 - Estrutura da DAG e das Tarefas

DAG: pipeline_produtos_vendas no Apache Airflow com 7 tarefas:

extract_produtos: lê e valida produtos_loja.csv.

extract_vendas: lê e valida vendas_produtos.csv.

transform_data: limpa e transforma dados, calcula Receita_Total, Margem_Lucro e Mes_Venda.

create_tables: cria tabelas no PostgreSQL.

load_data: insere dados nas tabelas.

generate_report: gera métricas de negócio.

baixa_performance (bônus): detecta produtos com menos de 2 vendas, registra em produtos_baixa_performance e imprime alerta no log.

Dependência das tarefas:

extract_produtos ┐
                 ├──> transform_data → create_tables → load_data → generate_report → baixa_performance
extract_vendas ┘

5 - Configuração da DAG

ID: pipeline_produtos_vendas

Agendamento: diário às 6h (0 6 * * *)

Tentativas de retry: 2

Email em falha: desativado

Tags: ['produtos', 'vendas', 'exercicio']

Data de início: 01/01/2025

Execução automática diária com controle de logs e tolerância a falhas.

6 - Relatórios e Resultados

Métricas geradas pelo generate_report:

Total de vendas por categoria.

Produto mais vendido.

Canal com maior receita (Online ou Loja Física).

Margem média por categoria.

Exemplo de saída:

{
  "Total por Categoria": {"Eletrônicos": 10550.0, "Acessórios": 2700.0},
  "Produto mais vendido": "Mouse Logitech",
  "Canal com maior receita": "Online",
  "Margem média por Categoria": {"Eletrônicos": 600.0, "Acessórios": 75.5}
}


Tarefa bônus (baixa_performance):

Detecta produtos com menos de 2 vendas.

Grava resultados em produtos_baixa_performance.

Imprime alerta no log para identificação de baixo giro de estoque.