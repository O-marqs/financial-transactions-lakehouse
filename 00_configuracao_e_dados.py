# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Gerar os dados
# MAGIC Vamos criar transações fictícias com Python e salvar em CSV.
# MAGIC Os valores estão em reais. Execute os notebooks na ordem 00 → 01 → 02 → 03.

# COMMAND ----------

CATALOGO = "workspace"  # Altere para o nome do seu catálogo.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"
ENTRADA = f"/Volumes/{CATALOGO}/{ESQUEMA}/entrada/dados_basicos"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BASE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {BASE}.entrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar uma lista de transações
# MAGIC Cada dicionário representa uma transação. O `for` repete a criação dos registros.

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)
transacoes = []
lojas = ["Amazon", "Starbucks", "Shell", "Netflix", "Uber"]

for numero in range(1, 5001):
    registro = {
        "id_transacao": f"TXN_{numero}",
        "id_usuario": f"USR_{random.randint(1, 500)}",
        "estabelecimento": random.choice(lojas),
        "valor": str(round(random.uniform(1, 1000), 2)),
        "data_transacao": str(date(2024, 1, 1) + timedelta(days=random.randint(0, 9))),
        "situacao": random.choice(["concluida", "pendente", "falhou"]),
    }
    # Alguns registros têm problemas para praticarmos a limpeza na Silver.
    if numero % 100 == 0:
        registro["valor"] = "-10.00"
    if numero % 100 == 1:
        registro["id_usuario"] = ""
    transacoes.append(registro)

# Adicionar cinco cópias iguais para praticarmos a remoção de duplicatas.
transacoes.extend(transacoes[:5])
dados = spark.createDataFrame(transacoes)
display(dados.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar em CSV
# MAGIC `header` inclui os nomes das colunas. `overwrite` substitui a amostra anterior.

# COMMAND ----------

dados.write.mode("overwrite").option("header", True).csv(ENTRADA)
print("Dados criados. Execute o notebook 01.")
