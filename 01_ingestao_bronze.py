# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ler o CSV
# MAGIC A Bronze guarda os dados recebidos, sem fazer a limpeza.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"  # Use o mesmo catálogo do notebook 00.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"
ENTRADA = f"/Volumes/{CATALOGO}/{ESQUEMA}/entrada/dados_basicos"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler os dados e registrar o horário
# MAGIC As colunas entram como texto. Os tipos serão ajustados na Silver.

# COMMAND ----------

bronze = spark.read.option("header", True).csv(ENTRADA)
bronze = bronze.withColumn("data_ingestao", F.current_timestamp())
display(bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar a tabela Delta
# MAGIC Delta é o formato das tabelas deste projeto.

# COMMAND ----------

bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{BASE}.bronze_transacoes"
)
print("Bronze salva. Execute o notebook 02.")
