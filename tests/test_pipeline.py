"""Regressões das funções dos notebooks, usando Spark local com ANSI ativo."""
import ast
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[1]
FUNCOES = {}
# Carrega definições para testar sem executar as células que acessam Databricks.
for nome in ["configuracao.py", "00_configuracao_e_dados.py", "02_limpeza_silver.py",
             "03_agregacoes_gold.py", "04_validacao.py"]:
    arvore = ast.parse((RAIZ / nome).read_text(encoding="utf-8"))
    definicoes = [no for no in arvore.body if isinstance(no, (ast.Import, ast.ImportFrom, ast.FunctionDef))
                  or (nome == "configuracao.py" and isinstance(no, ast.Assign))]
    exec(compile(ast.Module(body=definicoes, type_ignores=[]), nome, "exec"), FUNCOES)


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    sessao = (SparkSession.builder.master("local[2]").appName("teste-lakehouse")
              .config("spark.ui.enabled", "false")
              .config("spark.ui.showConsoleProgress", "false")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.sql.ansi.enabled", "true")
              .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def linha(identificador="t1", **alteracoes):
    registro = dict(id_transacao=identificador, id_usuario="u1", estabelecimento="Loja",
                    categoria="varejo", valor="10.00", moeda="USD", tipo_cartao="credito",
                    pais="BR", online="true", data_hora="2024-01-01 10:00:00", situacao="concluida")
    registro.update(alteracoes)
    return registro


def dataframe(spark, linhas):
    return spark.createDataFrame(linhas, FUNCOES["ESQUEMA_CSV"])


def test_geracao_reproduzivel_e_notebooks_validos():
    primeira = FUNCOES["gerar_transacoes"]()
    assert primeira == FUNCOES["gerar_transacoes"]()
    assert len(primeira) == 5000
    for caminho in RAIZ.glob("*.py"):
        texto = caminho.read_text(encoding="utf-8")
        compile(texto, str(caminho), "exec")
        if caminho.name[0].isdigit():
            assert "# MAGIC %run ./configuracao" in texto


def test_dados_invalidos_sao_preservados_na_quarentena(spark):
    defeitos = [dict(valor="abc"), dict(valor="NaN"), dict(valor="Infinity"),
                dict(valor="9999999999999999.99", moeda="GBP"), dict(valor=None),
                dict(valor="-10"), dict(valor="50000"), dict(id_usuario=" "),
                dict(id_transacao=None), dict(id_transacao=" "), dict(moeda=None),
                dict(moeda="XYZ"), dict(online="talvez"), dict(situacao=None),
                dict(data_hora="2024-02-30 10:00:00")]
    linhas = [linha(f"t{i}", **defeito) for i, defeito in enumerate(defeitos)]
    silver, quarentena = FUNCOES["limpar_e_separar"](dataframe(spark, linhas))
    assert silver.count() == 0
    rejeitadas = quarentena.collect()
    assert len(rejeitadas) == len(linhas)
    originais = {json.dumps(linha, sort_keys=True) for linha in linhas}
    assert {json.dumps(json.loads(r["_dados_originais"]), sort_keys=True) for r in rejeitadas} == originais
    assert all(r["_motivo"] and r["_aprovada"] is False for r in rejeitadas)


def test_cambio_decimal_e_duplicatas(spark):
    linhas = [linha("eur", moeda=" eur ", valor="10.10"),
              linha("cad", moeda="CAD", valor="10.00"),
              linha("dup"), linha("dup"), linha(None), linha(None)]
    silver, quarentena = FUNCOES["limpar_e_separar"](dataframe(spark, linhas))
    valores = {r["id_transacao"]: r["valor_usd"] for r in silver.collect()}
    assert valores == {"eur": Decimal("10.91"), "cad": Decimal("7.40")}
    assert quarentena.count() == 4
    assert silver.schema["valor_usd"].dataType.simpleString() == "decimal(18,2)"


def test_gold_conta_somente_concluidas_no_valor(spark):
    bronze = dataframe(spark, [linha("1"), linha("2", valor="20", situacao="pendente"),
                               linha("3", valor="30", situacao="falhou")])
    silver, _ = FUNCOES["limpar_e_separar"](bronze)
    lojas, dias = FUNCOES["calcular_indicadores"](silver)
    assert lojas.first()["valor_tentado_usd"] == Decimal("60.00")
    resultado = dias.first()
    assert resultado["valor_concluido_usd"] == Decimal("10.00")
    assert resultado["total_transacoes"] == 3
    assert resultado["percentual_falhas"] == 33.33


def test_amostra_completa_e_validacao(spark):
    bronze = dataframe(spark, FUNCOES["gerar_transacoes"]())
    silver, quarentena = FUNCOES["limpar_e_separar"](bronze)
    lojas, dias = FUNCOES["calcular_indicadores"](silver)
    resultado = FUNCOES["conferir_resultado"](bronze, silver, quarentena, lojas, dias)
    assert resultado == {"bronze": 5000, "silver": 4800, "quarentena": 200}
    assert dias.count() == 10
    with pytest.raises(ValueError, match="dias duplicados"):
        FUNCOES["conferir_resultado"](bronze, silver, quarentena, lojas, dias.unionByName(dias))


def test_sem_transacoes_aprovadas(spark):
    bronze = dataframe(spark, [linha(valor="abc")])
    silver, quarentena = FUNCOES["limpar_e_separar"](bronze)
    lojas, dias = FUNCOES["calcular_indicadores"](silver)
    resultado = FUNCOES["conferir_resultado"](bronze, silver, quarentena, lojas, dias)
    assert resultado == {"bronze": 1, "silver": 0, "quarentena": 1}
