# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: criar indicadores com SQL
# MAGIC A camada Gold resume os dados para análise.
# MAGIC Vamos usar SELECT, WHERE, GROUP BY, COUNT, SUM e AVG.
# MAGIC Os indicadores consideram apenas transações concluídas e os valores estão em BRL.

# COMMAND ----------

CATALOGO = "workspace"  # Use o mesmo catálogo do notebook 00.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"

# Uma visão temporária permite consultar o DataFrame nas células SQL abaixo.
spark.table(f"{BASE}.silver_transacoes").createOrReplaceTempView("transacoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo por estabelecimento

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW resumo_estabelecimentos AS
# MAGIC SELECT
# MAGIC   estabelecimento,
# MAGIC   categoria,
# MAGIC   COUNT(*) AS transacoes_concluidas,
# MAGIC   SUM(valor) AS valor_total_brl,
# MAGIC   ROUND(AVG(valor), 2) AS ticket_medio_brl
# MAGIC FROM transacoes
# MAGIC WHERE situacao = 'concluida'
# MAGIC GROUP BY estabelecimento, categoria

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo por dia

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW resumo_diario AS
# MAGIC SELECT
# MAGIC   data_transacao,
# MAGIC   COUNT(*) AS transacoes_concluidas,
# MAGIC   SUM(valor) AS valor_total_brl,
# MAGIC   COUNT(DISTINCT id_usuario) AS usuarios_ativos
# MAGIC FROM transacoes
# MAGIC WHERE situacao = 'concluida'
# MAGIC GROUP BY data_transacao

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar as tabelas Gold com PySpark

# COMMAND ----------

spark.table("resumo_estabelecimentos").write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{BASE}.gold_estabelecimentos")

spark.table("resumo_diario").write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{BASE}.gold_indicadores_diarios")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM resumo_estabelecimentos ORDER BY valor_total_brl DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM resumo_diario ORDER BY data_transacao
