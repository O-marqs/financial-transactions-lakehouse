# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: resumir os dados com SQL
# MAGIC Vamos contar e somar as transações concluídas por loja e por dia.
# MAGIC Os valores estão em reais.

# COMMAND ----------

CATALOGO = "workspace"  # Use o mesmo catálogo do notebook 00.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo por loja
# MAGIC `WHERE` escolhe as transações concluídas. `GROUP BY` reúne os registros de cada loja.
# MAGIC `COUNT` conta, `SUM` soma e `AVG` calcula a média.

# COMMAND ----------

resumo_lojas = spark.sql(f"""
    SELECT
        estabelecimento,
        COUNT(*) AS quantidade,
        SUM(valor) AS valor_total,
        ROUND(AVG(valor), 2) AS valor_medio
    FROM {BASE}.silver_transacoes
    WHERE situacao = 'concluida'
    GROUP BY estabelecimento
    ORDER BY valor_total DESC
""")
display(resumo_lojas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo por dia

# COMMAND ----------

resumo_diario = spark.sql(f"""
    SELECT
        data_transacao,
        COUNT(*) AS quantidade,
        SUM(valor) AS valor_total
    FROM {BASE}.silver_transacoes
    WHERE situacao = 'concluida'
    GROUP BY data_transacao
    ORDER BY data_transacao
""")
display(resumo_diario)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar as tabelas Gold

# COMMAND ----------

resumo_lojas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{BASE}.gold_estabelecimentos"
)
resumo_diario.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{BASE}.gold_indicadores_diarios"
)
print("Gold salva. Projeto concluído.")
