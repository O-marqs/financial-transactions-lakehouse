# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Gerar os dados
# MAGIC Vamos criar uma amostra de transações financeiras com Python e salvar em CSV.
# MAGIC Os dados são fictícios e os valores estão em reais (BRL).
# MAGIC Altere o catálogo abaixo e execute os notebooks na ordem 00 → 01 → 02 → 03.

# COMMAND ----------

CATALOGO = "workspace"  # Use o nome do catálogo do seu Databricks.
ESQUEMA = "fintech_lakehouse_pt"
BASE = f"`{CATALOGO}`.`{ESQUEMA}`"
ENTRADA = f"/Volumes/{CATALOGO}/{ESQUEMA}/entrada/dados_basicos"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BASE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {BASE}.entrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar a amostra com Python
# MAGIC A lista guarda os registros. O laço `for` gera uma transação por vez.
# MAGIC Incluímos valores negativos, usuários vazios e duplicatas para praticar a limpeza.

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)
transacoes = []
lojas = {"Amazon": "comercio", "Starbucks": "alimentacao", "Shell": "combustivel",
         "Netflix": "assinatura", "Uber": "transporte"}

for numero in range(1, 5001):
    loja = random.choice(list(lojas))
    registro = {
        "id_transacao": f"TXN_{numero:05d}",
        "id_usuario": f"USR_{random.randint(1, 500):05d}",
        "estabelecimento": loja,
        "categoria": lojas[loja],
        "valor": f"{random.randint(100, 100000) / 100:.2f}",
        "data_transacao": str(date(2024, 1, 1) + timedelta(days=(numero - 1) // 500)),
        "situacao": random.choice(["concluida", "concluida", "concluida", "pendente", "falhou"]),
    }
    if numero % 100 == 0:
        registro["valor"] = "-10.00"
    if numero % 100 == 1:
        registro["id_usuario"] = ""
    transacoes.append(registro)

# Repetir cinco registros iguais para demonstrar a remoção de duplicatas.
transacoes.extend(transacoes[:5])
dados = spark.createDataFrame(transacoes)
display(dados.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar os arquivos CSV no volume
# MAGIC `overwrite` substitui a amostra deste projeto quando executamos novamente.

# COMMAND ----------

dados.coalesce(1).write.mode("overwrite").option("header", True).csv(ENTRADA)
print(f"Amostra criada: {dados.count()} registros. Execute o notebook 01.")
