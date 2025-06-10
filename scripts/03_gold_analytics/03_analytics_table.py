from pyspark.sql.functions import col

def create_gold_analytics_orders_table():
    """
    Lê as tabelas da camada Silver, junta todas para criar uma tabela de fatos
    de pedidos e a salva na camada Gold, pronta para análise.
    """
    print("Iniciando a criação da tabela de análise da camada Gold...")

    # 1. Ler todas as tabelas da camada Silver necessárias do Catálogo
    df_orders = spark.table("default.silver_orders")
    df_order_items = spark.table("default.silver_order_items")
    df_products = spark.table("default.silver_products")
    df_customers = spark.table("default.silver_customers")

    # 2. Juntar as tabelas para criar a visão de negócio completa
    #    O resultado é uma tabela "larga" com informações de todas as entidades.
    df_gold_orders = df_orders.join(
      df_order_items, "order_id", "inner"  # Inner join garante que só teremos itens de pedidos que existem
    ).join(
      df_products, "product_id", "left"  # Left join para manter todos os pedidos, mesmo que um produto não seja encontrado
    ).join(
      df_customers, "customer_id", "left" # Left join para manter todos os pedidos, mesmo que um cliente não seja encontrado
    )

    # 3. Selecionar as colunas finais para a tabela de análise e criar novas métricas
    df_gold_analytics_orders = df_gold_orders.select(
        col("order_id"),
        col("customer_unique_id"),
        col("order_status"),
        col("purchase_timestamp"),
        col("approved_at"),
        col("delivered_customer_date"),
        col("estimated_delivery_date"),
        col("product_id"),
        col("product_category"),
        col("price"),
        col("freight_value"),
        # Criando uma nova coluna (métrica de negócio) que soma o preço e o frete
        (col("price") + col("freight_value")).alias("total_value"),
        col("customer_city"),
        col("customer_state")
    )

    # 4. Salvar a tabela final na camada Gold
    print("Salvando a tabela: default.gold_analytics_orders")
    df_gold_analytics_orders.write.format("delta") \
      .mode("overwrite") \
      .saveAsTable("default.gold_analytics_orders")
      
    print("\nProcesso da camada Gold concluído! Tabela 'gold_analytics_orders' pronta para BI.")


if __name__ == "__main__":
    create_gold_analytics_orders_table()
