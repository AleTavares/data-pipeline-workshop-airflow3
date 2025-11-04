1 - Identificação dos Problemas 

Os arquivos fornecidos (produtos_loja.csv e vendas_produtos.csv), tinham alguns valores ausentes que precisavam ser tratados antes de realizar qualquer cálculo ou carregamento no banco de dados.

No arquivo produtos_loja.csv, o campo Preco_Custo apresentou valor nulo no produto P003 (Teclado Mecânico). Além de que, o campo Fornecedor também estava nulo no produto P005 (Webcam HD). Esses valores ausentes poderiam gerar erros durante o cálculo de margens e custos, e prejudicar a consistência dos dados.

No arquivo vendas_produtos.csv, o campo Preco_Venda entregou um valor nulo na venda V005 (Produto P002). Esse campo é necessário para o cálculo de Receita_Total e Margem_Lucro.

2 - Estratégia de Tratamento dos Dados Nulos

Utilizei três estratégias para o tratamento de valores nulos:

Campo Preco_Custo (produtos_loja.csv):
Os valores nulos foram preenchidos com a média de preço de custo da categoria correspondente.
Essa estratégia mantém coerência entre produtos semelhantes e evita distorções nos cálculos. Por exemplo, se o custo de um teclado mecânico está ausente, ele vai ser preenchido com o custo médio dos outros produtos da categoria “Acessórios”.

Campo Fornecedor (produtos_loja.csv):
Os valores nulos foram substituídos pela string "Não Informado".
Esse método preserva a integridade da base de dados, permitindo consultas sem falhas.

Campo Preco_Venda (vendas_produtos.csv):
Os valores que faltavam de preço de venda foram calculados como Preco_Custo * 1.3, ou seja, aplicando uma margem padrão de 30%.


Essas transformações foram implementadas na função transform_data() do código Python utilizando a biblioteca Pandas.

3 - Definição da Estratégia ETL ou ELT

A estratégia adotada neste projeto foi ETL (Extract, Transform, Load), ou seja, Extrair, Transformar e depois Carregar os dados no banco.

A escolha foi feita pelos motivos a seguir:

O volume de dados é pequeno (apenas arquivos CSV), o que torna o processamento local mais eficiente;

Todas as transformações e limpezas são realizadas diretamente no Python (com Pandas), antes do carregamento no banco;

O PostgreSQL é utilizado apenas como repositório final, não sendo necessário delegar a ele o trabalho de transformação.


4 - Estrutura da DAG e das Tarefas

A DAG criada no Apache Airflow foi nomeada pipeline_produtos_vendas e contém sete tarefas principais:

extract_produtos: Lê o arquivo produtos_loja.csv, valida sua existência e registra o número de registros extraídos.

extract_vendas: Lê o arquivo vendas_produtos.csv e também valida sua existência.

transform_data: Realiza todas as transformações e limpezas de dados, calculando os campos Receita_Total, Margem_Lucro e Mes_Venda.

create_tables: Cria as tabelas produtos_processados, vendas_processadas e relatorio_vendas no banco PostgreSQL.

load_data: Insere os dados transformados nas tabelas criadas.

generate_report: Gera relatórios com métricas de negócio (total de vendas por categoria, produto mais vendido, canal com maior receita e margem média por categoria).

baixa_performance (tarefa bônus): Detecta produtos com menos de 2 vendas, imprime um alerta no log e grava o resultado em uma nova tabela chamada produtos_baixa_performance.

A dependência entre as tarefas foi configurada da seguinte forma:

extract_produtos ┐
              ├──> transform_data → create_tables → load_data → generate_report → baixa_performance
extract_vendas ┘

5 - Configuração da DAG

A DAG foi configurada com as seguintes propriedades:

ID da DAG: pipeline_produtos_vendas

Agendamento: diário às 6h da manhã (0 6 * * *)

Tentativas de retry: 2

Envio de e-mail em falha: desativado (email_on_failure=False)

Tags: ['produtos', 'vendas', 'exercicio']

Data de início: 01/01/2025

A DAG execute automaticamente todos os dias, com tolerância a falhas e controle de logs.

6 - Relatórios e Resultados

Após a execução completa do pipeline, o relatório gerado na etapa generate_report apresenta as seguintes métricas:

Total de vendas por categoria: soma da receita total agrupada por categoria de produto;

Produto mais vendido: item com maior quantidade de vendas;

Canal com maior receita: canal (Online ou Loja Física) que mais gerou faturamento;

Margem média por categoria: média da diferença entre preço de venda e preço de custo para cada categoria.

Exemplo de saída do relatório:

 RELATÓRIO GERADO:
{
  "Total por Categoria": {"Eletrônicos": 10550.0, "Acessórios": 2700.0},
  "Produto mais vendido": "Mouse Logitech",
  "Canal com maior receita": "Online",
  "Margem média por Categoria": {"Eletrônicos": 600.0, "Acessórios": 75.5}
}

Tarefa Bônus – Detecção de Produtos de Baixa Performance

A tarefa adicional chamada baixa_performance foi implementada para detectar produtos com desempenho de vendas baixo.

Ela realiza uma consulta na tabela vendas_processadas, soma as quantidades vendidas por produto e filtra todos os produtos cuja soma total seja menor que 2 vendas.
Esses produtos são então gravados em uma nova tabela chamada produtos_baixa_performance, junto com a quantidade vendida e a data de processamento.

Essa etapa também imprime no log do Airflow um alerta listando os produtos encontrados, ajudando na detecção de possíveis itens com baixo giro no estoque.

