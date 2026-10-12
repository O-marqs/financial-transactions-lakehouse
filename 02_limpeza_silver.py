# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: limpar os dados
# MAGIC Vamos ajustar os tipos, remover espaços e tirar os registros inválidos e duplicados.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"  # Use o mesmo catálogo do notebook 00.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"
bronze = spark.table(f"{BASE}.bronze_transacoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ajustar os tipos e remover espaços
# MAGIC `try_cast` converte o texto em número ou data. Se não conseguir, o resultado fica nulo.
# MAGIC `trim` remove espaços nas pontas do texto.

# COMMAND ----------

dados = bronze.withColumn("valor", F.expr("try_cast(valor AS DECIMAL(12,2))"))
dados = dados.withColumn("data_transacao", F.expr("try_cast(data_transacao AS DATE)"))
dados = dados.withColumn("id_transacao", F.trim("id_transacao"))
dados = dados.withColumn("id_usuario", F.trim("id_usuario"))
dados = dados.withColumn("estabelecimento", F.trim("estabelecimento"))
dados = dados.withColumn("situacao", F.lower(F.trim("situacao")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtrar e remover duplicatas
# MAGIC Cada linha aplica uma condição. Os dados recebidos continuam guardados na Bronze.

# COMMAND ----------

silver = dados.filter(F.col("valor") > 0)
silver = silver.filter(F.length("id_transacao") > 0)
silver = silver.filter(F.length("id_usuario") > 0)
silver = silver.filter(F.col("data_transacao").isNotNull())
silver = silver.filter(F.col("situacao").isin("concluida", "pendente", "falhou"))
silver = silver.dropDuplicates(["id_transacao"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar a Silver

# COMMAND ----------

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{BASE}.silver_transacoes"
)
display(silver.limit(5))
print("Silver salva. Execute o notebook 03.")
