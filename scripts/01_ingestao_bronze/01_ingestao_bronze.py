import re
# Nota: as bibliotecas 'pyspark' e 'dbutils' já estão disponíveis no ambiente Databricks

def run_bronze_ingestion():
  """
  Função principal que lê todos os arquivos CSV do diretório bronze no S3
  e os salva como tabelas Delta no Databricks.
  """
  print("Iniciando a ingestão da camada Bronze...")

  diretorio_bronze = "s3a://datalake-olist/bronze/"
  arquivos_s3 = dbutils.fs.ls(diretorio_bronze)

  for arquivo in arquivos_s3:
    if arquivo.name.endswith(".csv"):
      print(f"Processando arquivo: {arquivo.name}")

      df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(arquivo.path)

      nome_tabela_limpo = "bronze_" + re.sub(r'(\.csv|olist_|_dataset|_translation)', '', arquivo.name)
      nome_tabela_final = nome_tabela_limpo.replace("-", "_")
      
      print(f"Salvando na tabela: {nome_tabela_final}")
      
      df.write.format("delta").mode("overwrite").saveAsTable(nome_tabela_final)

  print("\nProcesso da camada Bronze concluído!")

if __name__ == "__main__":
  run_bronze_ingestion()
