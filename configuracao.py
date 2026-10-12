# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração do projeto
# MAGIC Edite apenas o catálogo abaixo e mantenha todos os notebooks na mesma pasta.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CATALOGO = "workspace"  # Confira o nome no explorador de catálogos do Databricks.
ESQUEMA = "fintech_lakehouse_pt"  # Use um esquema dedicado a este projeto de estudo.

# Aspas protegem nomes de catálogo com hífen. Caminhos de volume não aceitam /.
for nome in [CATALOGO, ESQUEMA]:
    if not nome or any(caractere in nome for caractere in ["/", "\\", "`", "."]):
        raise ValueError("Catálogo e esquema devem ser nomes simples, sem /, \\, ` ou ponto.")

BASE = f"`{CATALOGO}`.`{ESQUEMA}`"
ENTRADA = f"/Volumes/{CATALOGO}/{ESQUEMA}/entrada/amostra_v1"
BRONZE = f"{BASE}.bronze_transacoes"
SILVER = f"{BASE}.silver_transacoes"
QUARENTENA = f"{BASE}.silver_quarentena"
GOLD_ESTABELECIMENTOS = f"{BASE}.gold_estabelecimentos"
GOLD_DIARIO = f"{BASE}.gold_indicadores_diarios"
COLUNAS = ["id_transacao", "id_usuario", "estabelecimento", "categoria", "valor",
           "moeda", "tipo_cartao", "pais", "online", "data_hora", "situacao"]
ESQUEMA_CSV = StructType([StructField(nome, StringType(), True) for nome in COLUNAS])

spark.conf.set("spark.sql.session.timeZone", "UTC")
print(f"Catálogo: {CATALOGO} | Esquema: {ESQUEMA}")
