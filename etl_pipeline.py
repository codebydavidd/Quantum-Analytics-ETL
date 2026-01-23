import pandas as pd
import os
import time

![Imagem do projeto finalizado](prints/projeto.jpg)

# O script procura a pasta 'data' automaticamente no mesmo local onde ele está salvo
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def processar_dados():
    print("🚀 [INICIO] Iniciando Pipeline Quantum Analytics...")
    start_time = time.time()

    # 1. EXTRAÇÃO (EXTRACT)
    print("📂 [1/3] Carregando datasets brutos...")
    try:
        orders = pd.read_csv(os.path.join(DATA_DIR, 'olist_orders_dataset.csv'))
        items = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_items_dataset.csv'))
        products = pd.read_csv(os.path.join(DATA_DIR, 'olist_products_dataset.csv'))
        payments = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_payments_dataset.csv'))
    except FileNotFoundError as e:
        print(f"❌ Erro: Arquivo não encontrado. Verifique a pasta 'data'. Detalhe: {e}")
        return

    print("P [2/3] Processando e unificando tabelas...")
    
    # Juntando as tabelas principais 
    df = orders.merge(items, on='order_id', how='left')
    df = df.merge(products, on='product_id', how='left')
    df = df.merge(payments, on='order_id', how='left')

    # Limpeza de Datas
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    
    # Engenharia de Atributos
    df['mes_ano'] = df['order_purchase_timestamp'].dt.to_period('M')
    df['total_venda'] = df['price'] + df['freight_value']
    
    # O que fazer com Nulos
    df['product_category_name'] = df['product_category_name'].fillna('outros')
    
    # Filtra apenas pedidos entregues e colunas relevantes
    df_clean = df[df['order_status'] == 'delivered'].copy()
    
    cols_uteis = [
        'order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 
        'mes_ano', 'price', 'freight_value', 'total_venda', 
        'payment_type', 'payment_installments', 
        'product_category_name'
    ]
    # Filtra colunas 
    cols_finais = [c for c in cols_uteis if c in df_clean.columns]
    df_final = df_clean[cols_finais]

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

