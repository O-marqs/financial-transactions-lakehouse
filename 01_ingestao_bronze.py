# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: carregar os dados originais
# MAGIC A camada Bronze guarda os registros como chegaram do CSV, sem limpeza.
# MAGIC Também adicionamos a origem do arquivo e o horário de ingestão.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"  # Use o mesmo catálogo do notebook 00.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"
ENTRADA = f"/Volumes/{CATALOGO}/{ESQUEMA}/entrada/dados_basicos"

# COMMAND ----------

bronze = (
    spark.read.option("header", True).option("inferSchema", False).csv(ENTRADA)
    .select("*", F.col("_metadata.file_path").alias("arquivo_origem"))
    .withColumn("data_ingestao", F.current_timestamp())
)

bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{BASE}.bronze_transacoes"
)

display(spark.table(f"{BASE}.bronze_transacoes").limit(5))
print(f"Bronze: {bronze.count()} registros. Execute o notebook 02.")
