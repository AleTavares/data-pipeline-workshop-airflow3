-- Limpar tabelas existentes (opcional)
DROP TABLE IF EXISTS produtos_processados CASCADE;
DROP TABLE IF EXISTS vendas_processadas CASCADE;
DROP TABLE IF EXISTS relatorio_vendas CASCADE;
DROP TABLE IF EXISTS produtos_baixa_performance CASCADE;

-- Criar tabela produtos_processados
CREATE TABLE produtos_processados (
    ID_Produto VARCHAR(10) PRIMARY KEY,
    Nome_Produto VARCHAR(100) NOT NULL,
    Categoria VARCHAR(50),
    Preco_Custo DECIMAL(10,2),
    Fornecedor VARCHAR(100),
    Status VARCHAR(20),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar tabela vendas_processadas
CREATE TABLE vendas_processadas (
    ID_Venda VARCHAR(10) PRIMARY KEY,
    ID_Produto VARCHAR(10),
    Quantidade_Vendida INTEGER,
    Preco_Venda DECIMAL(10,2),
    Data_Venda DATE,
    Canal_Venda VARCHAR(20),
    Receita_Total DECIMAL(10,2),
    Mes_Venda VARCHAR(7),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ID_Produto) REFERENCES produtos_processados(ID_Produto)
);

-- Criar tabela relatorio_vendas
CREATE TABLE relatorio_vendas (
    ID_Venda VARCHAR(10) PRIMARY KEY,
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Quantidade_Vendida INTEGER,
    Receita_Total DECIMAL(10,2),
    Margem_Lucro DECIMAL(10,2),
    Canal_Venda VARCHAR(20),
    Mes_Venda VARCHAR(7),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar tabela produtos_baixa_performance (Bônus)
CREATE TABLE produtos_baixa_performance (
    ID_Produto VARCHAR(10) PRIMARY KEY,
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Preco_Custo DECIMAL(10,2),
    Total_Vendas INTEGER,
    Quantidade_Total INTEGER,
    Data_Analise TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar índices para melhor performance
CREATE INDEX idx_vendas_produto ON vendas_processadas(ID_Produto);
CREATE INDEX idx_vendas_data ON vendas_processadas(Data_Venda);
CREATE INDEX idx_relatorio_categoria ON relatorio_vendas(Categoria);
CREATE INDEX idx_relatorio_mes ON relatorio_vendas(Mes_Venda);
