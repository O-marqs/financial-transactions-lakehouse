# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ler os dados originais
# MAGIC Todas as colunas entram como texto. A origem do arquivo e o horário são adicionados.
# MAGIC Esta é uma leitura em lote, sem streaming.

# COMMAND ----------

# MAGIC %run ./configuracao

# COMMAND ----------

arquivos = [arquivo for arquivo in dbutils.fs.ls(ENTRADA) if arquivo.name.endswith(".csv")]
if not arquivos:
    raise ValueError("Nenhum CSV encontrado. Execute a etapa 00 primeiro.")

brutos = (
    spark.read.format("csv").schema(ESQUEMA_CSV)
    .option("header", "true").option("enforceSchema", "false")
    .option("mode", "FAILFAST").option("pathGlobFilter", "*.csv")
    .load(ENTRADA)
    .select("*", F.col("_metadata.file_path").alias("_arquivo_origem"))
)
# Cabeçalhos ou estruturas de CSV incompatíveis interrompem a leitura.
if brutos.count() == 0:
    raise ValueError("Os CSVs estão vazios. Confira a etapa 00.")
bronze = brutos.withColumn("_ingerido_em", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar a tabela Bronze
# MAGIC Cada execução substitui os dados desta tabela de estudo, preservando o histórico Delta.
# MAGIC Use o esquema dedicado. Nenhuma tabela é removida com DROP TABLE.

# COMMAND ----------

bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE)
print(f"Bronze: {spark.table(BRONZE).count():,} linhas")
display(spark.table(BRONZE).limit(5))
print("Execute 02_limpeza_silver.")
