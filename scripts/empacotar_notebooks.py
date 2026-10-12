"""Converte notebooks fonte Databricks em .ipynb e cria um ZIP para importação."""
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

RAIZ = Path(__file__).resolve().parents[1]


def converter(caminho):
    texto = caminho.read_text(encoding="utf-8").removeprefix("# Databricks notebook source\n")
    celulas = []
    for trecho in texto.split("# COMMAND ----------"):
        linhas = trecho.strip().splitlines()
        if not linhas:
            continue
        linhas = [linha.removeprefix("# MAGIC ") if linha.startswith("# MAGIC ")
                  else "" if linha == "# MAGIC" else linha for linha in linhas]
        markdown = linhas[0].strip() == "%md" or linhas[0].startswith("%md ")
        if markdown:
            primeira = linhas.pop(0).removeprefix("%md").strip()
            if primeira:
                linhas.insert(0, primeira)
        celula = {"cell_type": "markdown" if markdown else "code", "id": f"celula_{len(celulas):03d}", "metadata": {},
                  "source": [linha + "\n" for linha in linhas]}
        if not markdown:
            celula.update(execution_count=None, outputs=[])
        celulas.append(celula)
    return {"cells": celulas, "metadata": {"kernelspec": {
        "display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}


def main():
    destino = RAIZ / "dist"
    destino.mkdir(exist_ok=True)
    fontes = sorted(RAIZ.glob("[0-9][0-9]_*.py")) + [RAIZ / "configuracao.py"]
    pacote = destino / "notebooks_databricks_pt.zip"
    with ZipFile(pacote, "w", compression=ZIP_DEFLATED) as arquivo:
        for fonte in fontes:
            notebook = destino / fonte.with_suffix(".ipynb").name
            notebook.write_text(json.dumps(converter(fonte), ensure_ascii=False, indent=2), encoding="utf-8")
            arquivo.write(notebook, arcname=notebook.name)
    print(f"Pacote gerado: {pacote}")


if __name__ == "__main__":
    main()
