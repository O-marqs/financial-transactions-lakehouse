# Lakehouse de transações financeiras

Projeto **nível estagiário**, em português, para aprender PySpark, Delta Lake e a arquitetura medalhão no **Databricks Free Edition**.

Adaptação do [projeto de Abhinav Mandal](https://github.com/abhinav2627/financial-transactions-lakehouse), por Lucas Marques. O histórico original foi preservado na branch `copia-original`.

## O que o projeto faz

```text
CSV → Bronze → Silver → Gold
                  ↓
              Quarentena
```

- **Bronze:** recebe os dados originais como texto.
- **Silver:** limpa os dados, converte valores para USD e aplica regras de qualidade.
- **Quarentena:** guarda linhas inválidas e duplicadas, com o motivo e os dados originais.
- **Gold:** calcula indicadores por estabelecimento e por dia.

A execução é em lote: relê a amostra e atualiza as tabelas integralmente. Todos os dados são sintéticos. Não há streaming, API externa ou bibliotecas adicionais para instalar no Databricks.

## Executar no Databricks

1. Entre no seu [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).
2. Importe os **seis arquivos `.py` da raiz como notebooks**, com os mesmos nomes e na **mesma pasta** do Workspace.
3. No notebook `configuracao`, altere `CATALOGO = "workspace"` para o nome mostrado no explorador de catálogos. Você precisa poder criar esquema, volume e tabelas nesse catálogo.
4. Conecte os notebooks ao processamento **Serverless**.
5. Execute **00 → 01 → 02 → 03 → 04**, aguardando cada etapa terminar.

| Arquivo | Etapa |
|---|---|
| `configuracao.py` | Configuração compartilhada; os outros notebooks carregam com `%run` |
| `00_configuracao_e_dados.py` | Criar esquema e volume; gerar 5.000 transações em dez CSVs |
| `01_ingestao_bronze.py` | Ler os CSVs e salvar Bronze |
| `02_limpeza_silver.py` | Limpar e separar Silver e quarentena |
| `03_agregacoes_gold.py` | Calcular e salvar os indicadores |
| `04_validacao.py` | Conferir contagens, IDs únicos e totais financeiros |

Ao finalizar, a amostra padrão deve ter **5.000 linhas na Bronze, 4.800 na Silver e 200 na quarentena**. Esses números pertencem à amostra desta adaptação, não à amostra original.

### Pacote para importação

Também é possível importar notebooks `.ipynb`. Para gerar os seis notebooks nesse formato e um ZIP:

```powershell
python scripts/empacotar_notebooks.py
```

Extraia `dist/notebooks_databricks_pt.zip` e importe os seis `.ipynb` na mesma pasta do Workspace. Não é necessário importar as duas versões.

## Regras de qualidade

| Regra | Condição |
|---|---|
| Valor positivo | Valor em USD maior que zero |
| Limite de valor | Valor em USD menor que 50.000 |
| ID de transação | Preenchido após remover espaços |
| ID de usuário | Preenchido após remover espaços |
| Situação | `concluida`, `pendente` ou `falhou` |
| Data e hora | Válidas no formato `yyyy-MM-dd HH:mm:ss` |
| Moeda | USD, EUR, GBP ou CAD |
| Online | Texto `true` ou `false` |
| ID único | Não repetido na amostra |

Condições nulas reprovam a linha. Para simplificar, **todas as versões de um ID repetido vão à quarentena**. Nenhuma linha é descartada silenciosamente: **Bronze = Silver + quarentena**.

Valores monetários usam `DECIMAL(18,2)`. Taxas fixas de exemplo: EUR × 1,08; GBP × 1,27; CAD × 0,74; USD × 1. Não são cotações atuais. Datas são interpretadas em UTC.

A coluna `_dados_originais` conserva os valores recebidos, e `_motivo` explica as reprovações. CSVs com cabeçalho ou estrutura incorreta interrompem a Bronze; a quarentena recebe valores inválidos em CSVs bem formados.

## Indicadores

- `gold_estabelecimentos`: uma linha por estabelecimento, categoria e data; quantidade, valor tentado, valor concluído e valor médio.
- `gold_indicadores_diarios`: uma linha por data; quantidade, valor concluído, falhas, usuários ativos e percentual de falhas.

Valor tentado inclui todas as situações. Valor concluído inclui apenas `concluida`. Volume transacionado não representa receita da plataforma. Uma transação com situação `falhou` pode ter dados válidos e continuar na Silver.

## Repetir e testar

A etapa 00 gera sempre a mesma amostra e confere os arquivos existentes. Ela interrompe a execução se encontrar conteúdo divergente. Para outra amostra, use outro `ESQUEMA`.

As etapas 01, 02 e 03 **substituem os dados das tabelas do projeto**, mantendo o histórico Delta. Use o esquema dedicado `fintech_lakehouse_pt` e execute uma vez por vez. As tabelas são gravadas separadamente; se houver falha, repita a etapa afetada e as seguintes, terminando com a validação 04.

Os testes locais são opcionais para quem só vai usar Databricks. Exigem Python 3.10 ou 3.11 e Java 11 ou 17:

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest tests -q
```

Os testes verificam as transformações com Spark local e modo ANSI. **A execução no seu workspace Databricks ainda precisa ser feita**, para conferir permissões, volumes e tabelas Delta. A etapa 04 ajuda a conferir o resultado lá.

Detalhes das correções: [AUDITORIA.md](AUDITORIA.md). Créditos e origem: [ORIGEM.md](ORIGEM.md).
