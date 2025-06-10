from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType

def transform_orders_table():
    """Lê a tabela bronze_orders, aplica transformações e salva como silver_orders."""
    print("Processando: Tabela de Pedidos (orders)")
    df_bronze = spark.table("default.bronze_orders")
    
    df_silver = df_bronze.select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        to_timestamp(col("order_purchase_timestamp")).alias("purchase_timestamp"),
        to_timestamp(col("order_approved_at")).alias("approved_at"),
        to_timestamp(col("order_delivered_carrier_date")).alias("delivered_carrier_date"),
        to_timestamp(col("order_delivered_customer_date")).alias("delivered_customer_date"),
        to_timestamp(col("order_estimated_delivery_date")).alias("estimated_delivery_date")
    ).where(col("order_id").isNotNull())

    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("default.silver_orders")
    print("Salvo em: default.silver_orders")

def transform_order_items_table():
    """Lê a tabela bronze_order_items, aplica transformações e salva como silver_order_items."""
    print("Processando: Tabela de Itens do Pedido (order_items)")
    df_bronze = spark.table("default.bronze_order_items")

    df_silver = df_bronze.select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("seller_id"),
        to_timestamp(col("shipping_limit_date")).alias("shipping_limit_date"),
        col("price").cast(DoubleType()).alias("price"),
        col("freight_value").cast(DoubleType()).alias("freight_value")
    )

    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("default.silver_order_items")
    print("Salvo em: default.silver_order_items")

def transform_products_table():
    """Lê as tabelas de produtos e tradução, junta, transforma e salva como silver_products."""
    print("Processando: Tabela de Produtos (products)")
    df_products = spark.table("default.bronze_products")
    df_translation = spark.table("default.bronze_product_category_name")

    df_joined = df_products.join(df_translation, "product_category_name", "left")

    df_silver = df_joined.select(
        col("product_id"),
        col("product_category_name_english").alias("product_category"),
        col("product_name_lenght").cast(IntegerType()).alias("product_name_length"),
        col("product_description_lenght").cast(IntegerType()).alias("product_description_length"),
        col("product_photos_qty").cast(IntegerType()).alias("product_photos_qty"),
        col("product_weight_g").cast(IntegerType()),
        col("product_length_cm").cast(IntegerType()),
        col("product_height_cm").cast(IntegerType()),
        col("product_width_cm").cast(IntegerType())
    )
    
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("default.silver_products")
    print("Salvo em: default.silver_products")

def transform_customers_table():
    """Lê a tabela bronze_customers, seleciona colunas e salva como silver_customers."""
    print("Processando: Tabela de Clientes (customers)")
    df_bronze = spark.table("default.bronze_customers")

    df_silver = df_bronze.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state")
    )
    
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("default.silver_customers")
    print("Salvo em: default.silver_customers")


def run_silver_layer_transformations():
    """Função principal que orquestra a execução de todas as transformações da camada Silver."""
    print("Iniciando transformações da camada Silver...")
    transform_orders_table()
    transform_order_items_table()
    transform_products_table()
    transform_customers_table()
    print("\nProcesso da camada Silver concluído!")


if __name__ == "__main__":
    run_silver_layer_transformations()
