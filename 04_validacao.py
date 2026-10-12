# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Conferir o resultado
# MAGIC Verificação simples: contagens, IDs únicos e totais financeiros.
# MAGIC Execute após 00 → 01 → 02 → 03 e após qualquer reexecução.

# COMMAND ----------

# MAGIC %run ./configuracao

# COMMAND ----------


def conferir_resultado(bronze, silver, quarentena, estabelecimentos, diario):
    total_bronze = bronze.count()
    total_silver = silver.count()
    total_quarentena = quarentena.count()
    if total_bronze != total_silver + total_quarentena:
        raise ValueError("Há linhas perdidas ou contadas em dobro: Bronze deve ser Silver + quarentena.")
    if silver.select("id_transacao").distinct().count() != total_silver:
        raise ValueError("Há IDs duplicados na Silver.")
    if silver.filter("id_transacao IS NULL OR length(id_transacao) = 0").limit(1).count():
        raise ValueError("Há IDs vazios na Silver.")
    if quarentena.filter("_motivo IS NULL OR length(_motivo) = 0").limit(1).count():
        raise ValueError("Há linhas sem motivo na quarentena.")
    if diario.select("data_transacao").distinct().count() != diario.count():
        raise ValueError("Há dias duplicados nos indicadores Gold.")
    chaves = ["estabelecimento", "categoria", "data_transacao"]
    if estabelecimentos.select(*chaves).distinct().count() != estabelecimentos.count():
        raise ValueError("Há estabelecimentos duplicados nos indicadores Gold.")
    esperado = silver.agg(F.sum(F.when(F.col("situacao") == "concluida", F.col("valor_usd"))
                                  .otherwise(0)).alias("valor")).first()["valor"] or 0
    for tabela in [estabelecimentos, diario]:
        totais = tabela.agg(F.sum("total_transacoes").alias("linhas"),
                           F.sum("valor_concluido_usd").alias("valor")).first()
        if (totais["linhas"] or 0) != total_silver or (totais["valor"] or 0) != esperado:
            raise ValueError("Os totais da Gold não correspondem à Silver.")
    return {"bronze": total_bronze, "silver": total_silver, "quarentena": total_quarentena}


resultado = conferir_resultado(spark.table(BRONZE), spark.table(SILVER), spark.table(QUARENTENA),
                              spark.table(GOLD_ESTABELECIMENTOS), spark.table(GOLD_DIARIO))
for nome, valor in resultado.items():
    print(f"{nome}: {valor:,}")
print("Contagens e totais conferidos com sucesso.")
