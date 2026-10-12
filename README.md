# Lakehouse de transações financeiras

Este projeto organiza uma amostra de transações financeiras nas camadas **Bronze, Silver e Gold**. Os dados são gerados com Python, tratados com PySpark e resumidos com SQL no Databricks Free Edition.

## Fluxo dos dados

```text
CSV → Bronze → Silver → Gold
```

- **Bronze:** armazena os dados recebidos do CSV, com origem do arquivo e horário de ingestão.
- **Silver:** ajusta os tipos, limpa os textos e remove registros inválidos e duplicados.
- **Gold:** reúne indicadores de transações concluídas por estabelecimento e por dia.

O processamento é em lote. Os dados são fictícios e os valores estão em reais (BRL).

## Estrutura

| Notebook | Etapa |
|---|---|
| `00_configuracao_e_dados.py` | Criação do esquema, do volume e da amostra em CSV |
| `01_ingestao_bronze.py` | Leitura dos arquivos e gravação da tabela Bronze |
| `02_limpeza_silver.py` | Limpeza dos dados e gravação da tabela Silver |
| `03_agregacoes_gold.py` | Consultas SQL e gravação das tabelas Gold |

## Como executar

1. Importe os quatro arquivos `.py` como notebooks no Workspace do Databricks.
2. Ajuste `CATALOGO` nos quatro notebooks para o nome do seu catálogo. Mantenha o mesmo `ESQUEMA` em todos.
3. Conecte os notebooks ao processamento **Serverless**.
4. Execute na ordem **00 → 01 → 02 → 03**.

O usuário precisa ter permissão para criar um esquema, um volume e tabelas no catálogo escolhido. O esquema padrão é `fintech_lakehouse_pt`.

Cada execução substitui os arquivos e tabelas desta amostra. As tabelas são armazenadas em formato Delta, e os arquivos CSV ficam em um volume do Unity Catalog.

## Limpeza e resultados

A amostra contém 5.000 transações e cinco cópias repetidas. Algumas linhas têm valor negativo ou usuário vazio para exercitar a limpeza.

Na Silver, os valores são convertidos para decimal e as datas para o tipo date. São mantidos apenas registros com IDs preenchidos, valor positivo, data válida e situação conhecida. As duplicatas da amostra são iguais e apenas uma linha por ID é mantida.

Resultado conferido com Spark local:

| Camada | Registros |
|---|---:|
| Bronze | 5.005 |
| Silver | 4.900 |

Os registros recebidos continuam disponíveis na Bronze. A execução no workspace Databricks ainda precisa ser validada.

## Indicadores Gold

As consultas SQL usam `SELECT`, `WHERE`, `GROUP BY`, `COUNT`, `SUM`, `AVG` e `ORDER BY` para responder:

- Qual estabelecimento tem o maior valor total de transações concluídas?
- Qual é o valor médio dessas transações em cada estabelecimento?
- Quantas transações foram concluídas por dia?
- Quantos usuários realizaram transações concluídas em cada dia?

As tabelas geradas são `gold_estabelecimentos` e `gold_indicadores_diarios`. O valor transacionado representa volume financeiro, não receita da plataforma.
