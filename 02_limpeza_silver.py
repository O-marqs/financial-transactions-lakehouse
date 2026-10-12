# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: limpar e conferir a qualidade
# MAGIC Valores inválidos e duplicatas vão para a quarentena, com o motivo e os dados originais.
# MAGIC As conversões seguras evitam erros com o modo ANSI do Databricks.
# MAGIC O câmbio para USD usa taxas fixas de exemplo: EUR 1,08; GBP 1,27; CAD 0,74.

# COMMAND ----------

# MAGIC %run ./configuracao

# COMMAND ----------

from pyspark.sql.window import Window


def limpar_e_separar(bronze):
    # Conservar o conteúdo recebido antes das transformações.
    dados = bronze.withColumn("_dados_originais", F.to_json(F.struct(*COLUNAS),
                                                        options={"ignoreNullFields": "false"}))
    for coluna in ["id_transacao", "id_usuario", "estabelecimento", "categoria"]:
        dados = dados.withColumn(coluna, F.trim(F.col(coluna)))
    dados = (
        dados.withColumn("moeda", F.upper(F.trim(F.col("moeda"))))
        .withColumn("situacao", F.lower(F.trim(F.col("situacao"))))
        .withColumn("valor", F.expr("try_cast(valor AS DECIMAL(18,2))"))
        .withColumn("online", F.when(F.lower(F.trim(F.col("online"))) == "true", True)
                              .when(F.lower(F.trim(F.col("online"))) == "false", False))
        .withColumn("data_hora_transacao", F.try_to_timestamp("data_hora", F.lit("yyyy-MM-dd HH:mm:ss")))
        .withColumn("data_transacao", F.to_date("data_hora_transacao"))
        .drop("data_hora")
    )
    taxa = (F.when(F.col("moeda") == "USD", "1.00")
             .when(F.col("moeda") == "EUR", "1.08")
             .when(F.col("moeda") == "GBP", "1.27")
             .when(F.col("moeda") == "CAD", "0.74").cast("decimal(4,2)"))
    dados = (dados.withColumn("_taxa", taxa)
             .withColumn("valor_usd", F.expr("try_cast(valor * _taxa AS DECIMAL(18,2))"))
             .drop("_taxa"))
    # Regra simples: todas as versões de um ID repetido ficam na quarentena.
    dados = dados.withColumn("_repeticoes", F.count("*").over(Window.partitionBy("id_transacao")))
    regras = {
        "dq_valor_positivo": "valor_usd > 0",
        "dq_valor_limite": "valor_usd < 50000",
        "dq_id_transacao": "length(id_transacao) > 0",
        "dq_id_usuario": "length(id_usuario) > 0",
        "dq_situacao": "situacao IN ('concluida', 'pendente', 'falhou')",
        "dq_data_hora": "data_hora_transacao IS NOT NULL",
        "dq_moeda": "moeda IN ('USD', 'EUR', 'GBP', 'CAD')",
        "dq_online": "online IS NOT NULL",
        "dq_id_unico": "_repeticoes = 1",
    }
    for nome, condicao in regras.items():
        dados = dados.withColumn(nome, F.coalesce(F.expr(condicao), F.lit(False)))
    dados = dados.withColumn("_aprovada", F.expr(" AND ".join(regras)))
    silver = dados.filter("_aprovada").drop(*regras, "_aprovada", "_repeticoes", "_dados_originais")
    quarentena = (
        dados.filter(~F.col("_aprovada"))
        .withColumn("_motivo", F.concat_ws(", ", *[F.when(~F.col(nome), nome) for nome in regras]))
        .drop("_repeticoes")
    )
    return silver, quarentena


bronze = spark.table(BRONZE)
silver, quarentena = limpar_e_separar(bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar os resultados
# MAGIC As tabelas são atualizadas separadamente. Após uma interrupção, repita esta etapa.

# COMMAND ----------

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER)
quarentena.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(QUARENTENA)
print(f"Silver: {spark.table(SILVER).count():,} linhas")
print(f"Quarentena: {spark.table(QUARENTENA).count():,} linhas")
display(spark.table(QUARENTENA).groupBy("_motivo").count().orderBy(F.desc("count")))
print("Execute 03_agregacoes_gold.")
