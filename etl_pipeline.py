import pandas as pd
import os
import time

# O script procura a pasta 'data' automaticamente
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def processar_dados():
    print("🚀 [INICIO] Iniciando Pipeline Quantum Analytics (Versão Otimizada)...")
    start_time = time.time()

    # 1. EXTRAÇÃO (EXTRACT)
    print("📂 [1/3] Carregando datasets brutos...")
    try:
        orders = pd.read_csv(os.path.join(DATA_DIR, 'olist_orders_dataset.csv'))
        items = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_items_dataset.csv'))
        products = pd.read_csv(os.path.join(DATA_DIR, 'olist_products_dataset.csv'))
        payments = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_payments_dataset.csv'))
        translation = pd.read_csv(os.path.join(DATA_DIR, 'product_category_name_translation.csv'))
        
        # Tentativa de carregar a tabela de clientes (crucial para análise de retenção)
        try:
            customers = pd.read_csv(os.path.join(DATA_DIR, 'olist_customers_dataset.csv'))
            has_customers = True
        except FileNotFoundError:
            print("⚠️ Aviso: 'olist_customers_dataset.csv' não encontrado. Análise de clientes únicos ignorada.")
            has_customers = False

    except FileNotFoundError as e:
        print(f"❌ Erro: Arquivo não encontrado. Verifique a pasta 'data'. Detalhe: {e}")
        return

    print("⚙️ [2/3] Processando, limpando e unificando tabelas...")
    
    # A. Filtragem Inicial (Performance)
    # Filtramos pedidos entregues ANTES de fazer os joins para economizar memória
    orders_clean = orders[orders['order_status'] == 'delivered'].copy()
    
    # B. Agregação de Pagamentos (Evita Produto Cartesiano e duplicação de faturamento)
    payments_agg = payments.groupby('order_id', as_index=False)['payment_value'].sum()
    
    # C. Merges Estratégicos
    df = orders_clean.merge(items, on='order_id', how='left')
    df = df.merge(products, on='product_id', how='left')
    df = df.merge(payments_agg, on='order_id', how='left')
    
    # Adiciona a chave única do cliente, se o arquivo existir
    if has_customers:
        df = df.merge(customers[['customer_id', 'customer_unique_id']], on='customer_id', how='left')
        
    # D. Tradução das Categorias para Inglês (Padrão de Mercado)
    df = df.merge(translation, on='product_category_name', how='left')
    # Preenche nulos com 'others' (em inglês)
    df['product_category_name_english'] = df['product_category_name_english'].fillna('others')

    # E. Limpeza e Engenharia de Atributos
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['mes_ano'] = df['order_purchase_timestamp'].dt.to_period('M')
    
    # Calcula o total da venda baseado no preço do item + frete
    df['total_venda'] = df['price'] + df['freight_value']
    
    # Seleção Final de Colunas Estratégicas
    cols_uteis = [
        'order_id', 'customer_id', 'customer_unique_id', 'order_purchase_timestamp', 
        'mes_ano', 'price', 'freight_value', 'total_venda', 'payment_value',
        'product_category_name_english'
    ]
    
    # Filtra garantindo que colunas ausentes não quebrem o código
    cols_finais = [c for c in cols_uteis if c in df.columns]
    df_final = df[cols_finais]

    # 3. CARGAMENTO (LOAD)
    print("💾 [3/3] Salvando arquivo consolidado...")
    output_file = os.path.join(BASE_DIR, 'olist_master_analytics.csv')
    df_final.to_csv(output_file, index=False)

    end_time = time.time()
    print(f"\n✅ [SUCESSO] Pipeline finalizado em {end_time - start_time:.2f} segundos.")
    print(f"📊 Linhas processadas: {len(df_final)}")
    print(f"📁 Arquivo gerado: {output_file}")

if __name__ == "__main__":
    processar_dados()