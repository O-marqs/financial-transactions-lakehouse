# Transações financeiras com Databricks

Projeto de estudo com **Python, PySpark e SQL**, usando as camadas Bronze, Silver e Gold para organizar dados de transações financeiras.

Os dados são fictícios e os valores estão em reais.

## Como funciona

```text
CSV → Bronze → Silver → Gold
```

- **Bronze:** recebe os dados do CSV.
- **Silver:** ajusta os tipos e remove dados inválidos e duplicados.
- **Gold:** calcula a quantidade e o valor das transações concluídas por loja e por dia.

## Arquivos

| Notebook | O que faz |
|---|---|
| `00_configuracao_e_dados.py` | Gera a amostra com Python e salva em CSV |
| `01_ingestao_bronze.py` | Lê o CSV com PySpark e salva a Bronze |
| `02_limpeza_silver.py` | Limpa os dados e salva a Silver |
| `03_agregacoes_gold.py` | Usa SQL para criar os resumos da Gold |

## Como executar

1. Importe os quatro arquivos `.py` como notebooks no Databricks Free Edition.
2. Ajuste `CATALOGO` para o nome do seu catálogo em todos os notebooks.
3. Use o processamento **Serverless**.
4. Execute na ordem **00 → 01 → 02 → 03**.

Os notebooks criam o esquema `fintech_lakehouse_pt`, um volume para os CSVs e as tabelas Delta. Ao executar novamente, os dados deste projeto são substituídos.
