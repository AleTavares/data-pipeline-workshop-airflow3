Análise de Qualidade dos Dados

Problemas Identificados nos Dados

1. **dados_vendas.csv**

 Campos com Valores Nulos:
- **Valor** (linha 7): `P001,,1,2023-01-03,Norte` - Campo Valor está vazio

2. **vendas_produtos.csv**

Campos com Valores Nulos:
- **Preco_Venda** (linha 6): `V005,P002,10,,2024-01-17,Loja Física` - Campo Preco_Venda está vazio

3. **produtos_loja.csv**

 Campos com Valores Nulos:
- **Preco_Custo** (linha 4): `P003,Teclado Mecânico,Acessórios,,Razer,Inativo` - Campo Preco_Custo está vazio
- **Fornecedor** (linha 6): `P005,Webcam HD,Acessórios,120.00,,Ativo` - Campo Fornecedor está vazio

 Transformações Necessárias

Para **dados_vendas.csv**:

1. **Tratamento de Valores Nulos em Valor:**
   -  **Já implementado**: A DAG atual preenche com 0 (linha 50)
   -  **Melhoria sugerida**: Preencher com média/mediana do produto ou rejeitar registro

2. **Validação de Dados:**
   - Validar que Quantidade > 0
   - Validar que Data está em formato correto
   - Validar que Regiao está preenchida

3. **Transformações Adicionais:**
   -  Cálculo de TotalVenda (já implementado)
   - Adicionar validação de integridade referencial (se houver tabela de produtos)

Para **vendas_produtos.csv**:

1. **Tratamento de Valores Nulos em Preco_Venda:**
   - Buscar preço na tabela de produtos (produtos_loja.csv)
   - Se não encontrado, usar preço médio do produto
   - Se não existir histórico, rejeitar registro ou usar valor padrão

2. **Validações:**
   - Validar que Quantidade_Vendida > 0
   - Validar formato de Data_Venda
   - Validar que Canal_Venda está preenchido

 Para **produtos_loja.csv**:

1. **Tratamento de Valores Nulos em Preco_Custo:**
   - Se produto inativo, pode manter nulo
   - Se produto ativo, buscar preço de referência ou usar valor padrão da categoria
   - Adicionar flag de qualidade de dados

2. **Tratamento de Valores Nulos em Fornecedor:**
   - Preencher com "Não informado" ou valor padrão
   - Criar categoria "Fornecedor Desconhecido"

3. **Validações:**
   - Validar que ID_Produto é único
   - Validar que Categoria está preenchida
   - Validar que Status está em valores permitidos (Ativo/Inativo)

---

 Resumo dos Campos com Valores Nulos

| Arquivo | Campo | Linha | Quantidade |
|---------|-------|-------|------------|
| dados_vendas.csv | Valor | 7 | 1 |
| vendas_produtos.csv | Preco_Venda | 6 | 1 |
| produtos_loja.csv | Preco_Custo | 4 | 1 |
| produtos_loja.csv | Fornecedor | 6 | 1 |

**Total de registros com valores nulos: 4**

---

 Recomendações de Melhorias

1. **Estratégia de Preenchimento:**
   - Valores numéricos: usar média/mediana ao invés de 0
   - Valores de texto: usar "Não informado" ou buscar de fonte alternativa

2. **Validação de Qualidade:**
   - Adicionar métricas de qualidade de dados
   - Registrar registros rejeitados em log separado

3. **Enriquecimento de Dados:**
   - Integrar dados de vendas_produtos.csv e produtos_loja.csv
   - Buscar preços faltantes em tabelas relacionadas

4. **Tratamento de Erros:**
   - Implementar notificações para valores nulos críticos
   - Criar alertas quando taxa de nulos exceder threshold

