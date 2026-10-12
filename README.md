# Lakehouse de transações financeiras

Projeto de portfólio para demonstrar conhecimentos básicos de **Python, SQL, PySpark, Delta Lake e arquitetura medalhão**, utilizando o **Databricks Free Edition**.

## Objetivo

Transformar dados fictícios de transações financeiras em tabelas para análise, seguindo o fluxo:

```text
CSV → Bronze → Silver → Gold
```

- **Bronze:** leitura dos dados originais e inclusão de informações de ingestão.
- **Silver:** conversão de tipos, limpeza de textos e remoção de registros inválidos e duplicados.
- **Gold:** consultas SQL com indicadores por estabelecimento e por dia.

A amostra contém 5.000 transações e cinco registros repetidos. Os valores estão em reais (BRL). O processamento é em lote.

## Notebooks

| Arquivo | Conhecimentos demonstrados |
|---|---|
| `00_configuracao_e_dados.py` | Listas, dicionários, laços em Python e geração de CSV |
| `01_ingestao_bronze.py` | Leitura de CSV, DataFrame e escrita de tabela Delta |
| `02_limpeza_silver.py` | `withColumn`, conversão de tipos, `filter` e `dropDuplicates` |
| `03_agregacoes_gold.py` | `SELECT`, `WHERE`, `GROUP BY`, `COUNT`, `SUM`, `AVG` e `ORDER BY` |

## Como executar

1. Importe os quatro arquivos `.py` como notebooks no seu Workspace do Databricks.
2. Ajuste `CATALOGO` nos quatro notebooks para o nome do seu catálogo. Use o mesmo `ESQUEMA` em todos.
3. Conecte ao processamento **Serverless** e execute **00 → 01 → 02 → 03**.

A etapa 00 cria um esquema e um volume de entrada no Unity Catalog. As outras etapas gravam tabelas Delta. Seu usuário precisa ter permissão para criar esses objetos.

Cada execução substitui os arquivos e tabelas desta amostra no esquema dedicado `fintech_lakehouse_pt`. Na amostra padrão, a Bronze possui **5.005 registros** e a Silver **4.900** após a limpeza. As linhas originais permanecem na Bronze.

## O que analisar

- Quais estabelecimentos tiveram maior valor total de transações concluídas?
- Quantas transações foram concluídas por dia?
- Quantos usuários realizaram transações concluídas em cada dia?

Os indicadores usam apenas transações com situação `concluida`. O valor transacionado representa volume financeiro, não receita da plataforma.

## Descrição para currículo

Pipeline em lote de transações financeiras com Python, PySpark e SQL, organizado nas camadas Bronze, Silver e Gold, com limpeza de dados e armazenamento em tabelas Delta.

## Sobre o projeto

Os dados são sintéticos, criados para estudo. A lógica foi verificada com Spark local; a execução no workspace Databricks ainda precisa ser validada. As duplicatas geradas são cópias iguais, e o projeto não trata versões conflitantes de uma mesma transação.

Adaptação em português por **Lucas Marques**, baseada no [projeto de Abhinav Mandal](https://github.com/abhinav2627/financial-transactions-lakehouse). O histórico e a branch `copia-original` preservam a origem. O README original declara licença MIT, sem arquivo `LICENSE` no commit copiado.
