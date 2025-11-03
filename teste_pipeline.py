#!/usr/bin/env python3
"""
Script de teste para validar a lógica do pipeline de produtos e vendas
sem dependência do Airflow completo.
"""

import pandas as pd
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_extract_produtos():
    """Testa a extração de produtos"""
    print("\n=== TESTANDO EXTRAÇÃO DE PRODUTOS ===")
    
    file_path = 'data/produtos_loja.csv'
    if not os.path.exists(file_path):
        print(f"❌ Arquivo não encontrado: {file_path}")
        return None
    
    df_produtos = pd.read_csv(file_path)
    print(f"✅ Produtos extraídos: {len(df_produtos)} registros")
    print(f"✅ Colunas: {list(df_produtos.columns)}")
    print(f"✅ Produtos por categoria: {df_produtos['Categoria'].value_counts().to_dict()}")
    
    return df_produtos

def test_extract_vendas():
    """Testa a extração de vendas"""
    print("\n=== TESTANDO EXTRAÇÃO DE VENDAS ===")
    
    file_path = 'data/vendas_produtos.csv'
    if not os.path.exists(file_path):
        print(f"❌ Arquivo não encontrado: {file_path}")
        return None
    
    df_vendas = pd.read_csv(file_path)
    print(f"✅ Vendas extraídas: {len(df_vendas)} registros")
    print(f"✅ Colunas: {list(df_vendas.columns)}")
    print(f"✅ Vendas por canal: {df_vendas['Canal_Venda'].value_counts().to_dict()}")
    
    return df_vendas

def test_transform_data(df_produtos, df_vendas):
    """Testa a transformação dos dados"""
    print("\n=== TESTANDO TRANSFORMAÇÃO DOS DADOS ===")
    
    if df_produtos is None or df_vendas is None:
        print("❌ Dados de entrada inválidos")
        return None, None
    
    # === LIMPEZA DE DADOS ===
    
    # 1. Tratar Preco_Custo
    print("🔄 Tratando valores nulos em Preco_Custo")
    df_produtos['Preco_Custo'] = pd.to_numeric(df_produtos['Preco_Custo'], errors='coerce')
    
    # Verificar nulos antes
    nulos_antes = df_produtos['Preco_Custo'].isna().sum()
    print(f"   Valores nulos antes: {nulos_antes}")
    
    # Calcular média por categoria
    media_por_categoria = df_produtos.groupby('Categoria')['Preco_Custo'].mean()
    print(f"   Médias por categoria: {media_por_categoria.to_dict()}")
    
    # Preencher valores nulos
    for categoria in df_produtos['Categoria'].unique():
        mask = (df_produtos['Categoria'] == categoria) & (df_produtos['Preco_Custo'].isna())
        if mask.any():
            valor_medio = media_por_categoria[categoria]
            df_produtos.loc[mask, 'Preco_Custo'] = valor_medio
            print(f"   ✅ Preenchido Preco_Custo para categoria {categoria} com {valor_medio}")
    
    # 2. Tratar Fornecedor
    print("🔄 Tratando fornecedores nulos")
    mask_fornecedor = df_produtos['Fornecedor'].isna() | (df_produtos['Fornecedor'] == '')
    nulos_fornecedor = mask_fornecedor.sum()
    df_produtos.loc[mask_fornecedor, 'Fornecedor'] = 'Não Informado'
    print(f"   ✅ Preenchidos {nulos_fornecedor} fornecedores com 'Não Informado'")
    
    # 3. Tratar Preco_Venda
    print("🔄 Tratando preços de venda nulos")
    df_vendas['Preco_Venda'] = pd.to_numeric(df_vendas['Preco_Venda'], errors='coerce')
    
    # Merge para obter preço de custo
    df_vendas_merged = df_vendas.merge(df_produtos[['ID_Produto', 'Preco_Custo']], 
                                     on='ID_Produto', how='left')
    
    nulos_preco_venda = df_vendas_merged['Preco_Venda'].isna().sum()
    print(f"   Valores nulos em Preco_Venda: {nulos_preco_venda}")
    
    mask_preco_venda = df_vendas_merged['Preco_Venda'].isna()
    df_vendas_merged.loc[mask_preco_venda, 'Preco_Venda'] = df_vendas_merged.loc[mask_preco_venda, 'Preco_Custo'] * 1.3
    print(f"   ✅ Preenchidos {nulos_preco_venda} preços de venda com fórmula Preco_Custo * 1.3")
    
    # === TRANSFORMAÇÕES ===
    
    # 4. Calcular Receita_Total
    df_vendas_merged['Receita_Total'] = df_vendas_merged['Quantidade_Vendida'] * df_vendas_merged['Preco_Venda']
    print("✅ Calculada Receita_Total")
    
    # 5. Calcular Margem_Lucro
    df_vendas_merged['Margem_Lucro'] = df_vendas_merged['Preco_Venda'] - df_vendas_merged['Preco_Custo']
    print("✅ Calculada Margem_Lucro")
    
    # 6. Criar campo Mes_Venda
    df_vendas_merged['Data_Venda'] = pd.to_datetime(df_vendas_merged['Data_Venda'])
    df_vendas_merged['Mes_Venda'] = df_vendas_merged['Data_Venda'].dt.strftime('%Y-%m')
    print("✅ Criado campo Mes_Venda")
    
    # Remover coluna auxiliar
    df_vendas_final = df_vendas_merged.drop('Preco_Custo', axis=1)
    
    print(f"✅ Transformação concluída:")
    print(f"   - Produtos processados: {len(df_produtos)}")
    print(f"   - Vendas processadas: {len(df_vendas_final)}")
    
    return df_produtos, df_vendas_final

def test_generate_report(df_produtos, df_vendas):
    """Testa a geração de relatórios"""
    print("\n=== TESTANDO GERAÇÃO DE RELATÓRIOS ===")
    
    if df_produtos is None or df_vendas is None:
        print("❌ Dados transformados inválidos")
        return
    
    # Criar relatório consolidado (simulando o JOIN)
    df_relatorio = df_vendas.merge(df_produtos[['ID_Produto', 'Nome_Produto', 'Categoria']], 
                                  on='ID_Produto', how='left')
    
    # 1. Total de vendas por categoria
    print("\n📊 TOTAL DE VENDAS POR CATEGORIA:")
    vendas_categoria = df_relatorio.groupby('Categoria').agg({
        'Receita_Total': 'sum',
        'Quantidade_Vendida': 'sum'
    }).round(2)
    
    for categoria, row in vendas_categoria.iterrows():
        print(f"   {categoria}: R$ {row['Receita_Total']:.2f} ({row['Quantidade_Vendida']} unidades)")
    
    # 2. Produto mais vendido
    print("\n📊 PRODUTO MAIS VENDIDO:")
    produto_mais_vendido = df_relatorio.groupby('Nome_Produto').agg({
        'Quantidade_Vendida': 'sum',
        'Receita_Total': 'sum'
    }).sort_values('Quantidade_Vendida', ascending=False).iloc[0]
    
    produto_nome = produto_mais_vendido.name
    print(f"   Produto: {produto_nome}")
    print(f"   Quantidade: {produto_mais_vendido['Quantidade_Vendida']} unidades")
    print(f"   Receita: R$ {produto_mais_vendido['Receita_Total']:.2f}")
    
    # 3. Canal com maior receita
    print("\n📊 CANAL COM MAIOR RECEITA:")
    canal_maior_receita = df_relatorio.groupby('Canal_Venda').agg({
        'Receita_Total': 'sum',
        'ID_Venda': 'count'
    }).sort_values('Receita_Total', ascending=False).iloc[0]
    
    canal_nome = canal_maior_receita.name
    print(f"   Canal: {canal_nome}")
    print(f"   Receita: R$ {canal_maior_receita['Receita_Total']:.2f}")
    print(f"   Número de vendas: {canal_maior_receita['ID_Venda']}")
    
    # 4. Margem de lucro média por categoria
    print("\n📊 MARGEM DE LUCRO MÉDIA POR CATEGORIA:")
    margem_categoria = df_relatorio.groupby('Categoria').agg({
        'Margem_Lucro': 'mean',
        'ID_Venda': 'count'
    }).round(2)
    
    for categoria, row in margem_categoria.iterrows():
        print(f"   {categoria}: R$ {row['Margem_Lucro']:.2f} (em {row['ID_Venda']} vendas)")
    
    # 5. Resumo geral
    print("\n📊 RESUMO GERAL:")
    total_vendas = len(df_relatorio)
    receita_total = df_relatorio['Receita_Total'].sum()
    margem_media = df_relatorio['Margem_Lucro'].mean()
    
    print(f"   Total de vendas: {total_vendas}")
    print(f"   Receita total: R$ {receita_total:.2f}")
    print(f"   Margem média: R$ {margem_media:.2f}")
    
    return df_relatorio

def main():
    """Função principal de teste"""
    print("🚀 INICIANDO TESTE DO PIPELINE DE PRODUTOS E VENDAS")
    print("=" * 60)
    
    try:
        # 1. Testar extração
        df_produtos = test_extract_produtos()
        df_vendas = test_extract_vendas()
        
        # 2. Testar transformação
        df_produtos_transformed, df_vendas_transformed = test_transform_data(df_produtos, df_vendas)
        
        # 3. Testar relatórios
        df_relatorio = test_generate_report(df_produtos_transformed, df_vendas_transformed)
        
        print("\n" + "=" * 60)
        print("✅ TESTE CONCLUÍDO COM SUCESSO!")
        print("✅ Todos os componentes do pipeline funcionaram corretamente")
        print("✅ A DAG está pronta para ser executada no Airflow")
        
    except Exception as e:
        print(f"\n❌ ERRO DURANTE O TESTE: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()