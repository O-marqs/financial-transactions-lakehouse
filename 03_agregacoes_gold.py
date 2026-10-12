# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: indicadores simples
# MAGIC Uma tabela por estabelecimento, categoria e dia; outra por dia.
# MAGIC Valor tentado inclui todas as situações. Valor concluído inclui apenas `concluida`.
# MAGIC Valor transacionado não é receita da plataforma: não temos dados de tarifas ou custos.

# COMMAND ----------

# MAGIC %run ./configuracao

# COMMAND ----------


def calcular_indicadores(silver):
    estabelecimentos = silver.groupBy("estabelecimento", "categoria", "data_transacao").agg(
        F.count("*").alias("total_transacoes"),
        F.sum("valor_usd").alias("valor_tentado_usd"),
        F.sum(F.when(F.col("situacao") == "concluida", F.col("valor_usd")).otherwise(0))
            .alias("valor_concluido_usd"),
        F.round(F.avg("valor_usd"), 2).alias("valor_medio_usd"),
    )
    diario = silver.groupBy("data_transacao").agg(
        F.count("*").alias("total_transacoes"),
        F.sum(F.when(F.col("situacao") == "concluida", F.col("valor_usd")).otherwise(0))
            .alias("valor_concluido_usd"),
        F.sum(F.when(F.col("situacao") == "falhou", 1).otherwise(0)).alias("total_falhas"),
        F.countDistinct("id_usuario").alias("usuarios_ativos"),
    ).withColumn("percentual_falhas", F.round(F.col("total_falhas") / F.col("total_transacoes") * 100, 2))
    return estabelecimentos, diario


silver = spark.table(SILVER)
estabelecimentos, diario = calcular_indicadores(silver)

# COMMAND ----------

estabelecimentos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_ESTABELECIMENTOS)
diario.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DIARIO)
display(spark.table(GOLD_ESTABELECIMENTOS).orderBy(F.desc("valor_concluido_usd")))
display(spark.table(GOLD_DIARIO).orderBy("data_transacao"))
print("Execute 04_validacao.")
