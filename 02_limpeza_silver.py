# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: limpar os dados
# MAGIC Vamos ajustar os tipos, remover espaços, filtrar registros inválidos e tirar duplicatas.
# MAGIC Os registros originais continuam disponíveis na Bronze.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"  # Use o mesmo catálogo do notebook 00.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"
bronze = spark.table(f"{BASE}.bronze_transacoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ajustar os tipos e os textos
# MAGIC `try_cast` transforma um valor inválido em nulo, permitindo filtrar a linha depois.
# MAGIC Usamos decimal para dinheiro e date para a data da transação.

# COMMAND ----------

tratados = (
    bronze.withColumn("valor", F.expr("try_cast(valor AS DECIMAL(12,2))"))
    .withColumn("data_transacao", F.expr("try_cast(data_transacao AS DATE)"))
    .withColumn("id_transacao", F.trim(F.col("id_transacao")))
    .withColumn("id_usuario", F.trim(F.col("id_usuario")))
    .withColumn("estabelecimento", F.trim(F.col("estabelecimento")))
    .withColumn("categoria", F.lower(F.trim(F.col("categoria"))))
    .withColumn("situacao", F.lower(F.trim(F.col("situacao"))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manter os registros válidos
# MAGIC IDs preenchidos, valor positivo, data válida e situação conhecida.
# MAGIC As duplicatas da amostra são cópias iguais; mantemos apenas uma por ID.

# COMMAND ----------

silver = (
    tratados.filter(
        (F.length("id_transacao") > 0)
        & (F.length("id_usuario") > 0)
        & (F.col("valor") > 0)
        & F.col("data_transacao").isNotNull()
        & F.col("situacao").isin("concluida", "pendente", "falhou")
    )
    .dropDuplicates(["id_transacao"])
)

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{BASE}.silver_transacoes"
)

print(f"Antes da limpeza: {bronze.count()} registros")
print(f"Depois da limpeza: {silver.count()} registros")
display(spark.table(f"{BASE}.silver_transacoes").limit(5))
print("Execute o notebook 03.")
