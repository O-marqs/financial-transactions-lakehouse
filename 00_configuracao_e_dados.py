# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Criar a estrutura e gerar dados
# MAGIC Projeto de estágio: CSV → Bronze → Silver e quarentena → Gold.
# MAGIC Importe os seis notebooks na mesma pasta, ajuste o catálogo em `configuracao`
# MAGIC e conecte ao processamento **Serverless**. Execute 00 → 01 → 02 → 03 → 04.

# COMMAND ----------

# MAGIC %run ./configuracao

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BASE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {BASE}.entrada")
dbutils.fs.mkdirs(ENTRADA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerar 5.000 transações sintéticas
# MAGIC A semente fixa e os IDs numerados permitem repetir a mesma amostra.
# MAGIC Alguns valores negativos, nulos e duplicados são incluídos para testar a qualidade.

# COMMAND ----------

import csv
import io
import random
from datetime import datetime, timedelta


def gerar_transacoes():
    gerador = random.Random(42)
    lojas = {"Amazon": "comercio_eletronico", "Starbucks": "alimentacao",
             "Shell": "combustivel", "Netflix": "assinatura", "Uber": "transporte"}
    transacoes = []
    for indice in range(5000):
        loja = gerador.choice(list(lojas))
        valor = round(gerador.uniform(1, 1000), 2)
        horario = datetime(2024, 1, 1) + timedelta(days=indice // 500,
                                                  seconds=gerador.randint(0, 86399))
        linha = {
            "id_transacao": f"TXN_{indice:05d}",
            "id_usuario": f"USR_{gerador.randint(1, 500):05d}",
            "estabelecimento": loja, "categoria": lojas[loja], "valor": f"{valor:.2f}",
            "moeda": gerador.choice(["USD", "EUR", "GBP", "CAD"]),
            "tipo_cartao": gerador.choice(["credito", "debito"]), "pais": "BR",
            "online": gerador.choice(["true", "false"]),
            "data_hora": horario.strftime("%Y-%m-%d %H:%M:%S"),
            "situacao": gerador.choices(["concluida", "pendente", "falhou"], [90, 7, 3])[0],
        }
        if indice % 100 == 0:
            linha["valor"] = "-10.00"
        elif indice % 100 == 1:
            linha["id_usuario"] = ""
        elif indice % 100 == 2:
            linha["valor"] = "valor_invalido"
        elif indice % 100 == 3:
            linha["id_transacao"] = transacoes[-1]["id_transacao"]
        transacoes.append(linha)
    return transacoes


transacoes = gerar_transacoes()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravar dez arquivos CSV
# MAGIC Repetir esta etapa confere os arquivos existentes. Ela não substitui conteúdo divergente.

# COMMAND ----------

existentes = {arquivo.name for arquivo in dbutils.fs.ls(ENTRADA)}
nomes_esperados = {f"lote_{numero:02d}.csv" for numero in range(10)}
if existentes - nomes_esperados:
    raise ValueError("Há arquivos inesperados na entrada. Use outro esquema para uma nova amostra.")

for numero in range(10):
    arquivo = io.StringIO(newline="")
    escritor = csv.DictWriter(arquivo, fieldnames=COLUNAS, lineterminator="\n")
    escritor.writeheader()
    escritor.writerows(transacoes[numero * 500:(numero + 1) * 500])
    conteudo = arquivo.getvalue()
    nome = f"lote_{numero:02d}.csv"
    caminho = f"{ENTRADA}/{nome}"
    if nome in existentes:
        if dbutils.fs.head(caminho, maxBytes=len(conteudo.encode("utf-8")) + 1) != conteudo:
            raise ValueError(f"Conteúdo divergente em {nome}. Use outro esquema para uma nova amostra.")
    else:
        dbutils.fs.put(caminho, conteudo, overwrite=False)
    print(f"Lote conferido: {nome}")

print("5.000 transações prontas. Execute 01_ingestao_bronze.")
