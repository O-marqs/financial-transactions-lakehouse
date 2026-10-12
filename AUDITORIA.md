# Auditoria do projeto

Revisão de 12 de outubro de 2026. Escopo: corrigir erros relevantes, adaptar ao Databricks Free Edition e deixar o material simples para nível de estágio.

## Correções

| Problema original | Solução |
|---|---|
| `input_file_name()` não funciona em Unity Catalog | Usar `_metadata.file_path` na leitura dos CSVs |
| Conversões inválidas interrompem a execução com ANSI ativo | Usar `try_cast`, `try_to_timestamp` e conversão booleana explícita |
| Regras nulas podem fazer linhas desaparecerem dos filtros | Tratar NULL como reprovação |
| Deduplicação elimina linhas sem registrar o motivo | Enviar todos os IDs repetidos à quarentena |
| IDs vazios são aceitos | Remover espaços e exigir conteúdo |
| Moeda é convertida antes da normalização | Remover espaços e converter para maiúsculas antes do câmbio |
| Dinheiro usa ponto flutuante | Usar valores decimais em centavos |
| Tabelas são removidas antes da escrita | Atualizar com `overwrite`, preservando o histórico Delta |
| Configuração repetida pode divergir | Manter apenas um notebook de configuração |
| README referencia pasta inexistente | Documentar a estrutura real e oferecer pacote de importação |

## Simplificações para nível de estágio

O fluxo mantém cinco etapas visíveis e uma configuração curta. Foram retirados indicadores avançados, como ranking de categoria líder e relatório com janelas e taxas do período. Assim, também deixam de existir os problemas de empate de categoria e média incorreta das taxas diárias presentes no relatório original.

Os indicadores finais são apenas quantidade, valores, falhas e usuários. São nove regras de qualidade. Não há escolha automática entre versões conflitantes de uma transação: todas as versões repetidas ficam na quarentena, que é uma regra fácil de explicar e conferir.

A amostra usa IDs numerados e uma semente fixa. Há exatamente 5.000 linhas, incluindo defeitos deliberados; o resultado esperado é 4.800 linhas Silver e 200 na quarentena. O notebook 04 confere reconciliação, IDs únicos, motivos da quarentena, chaves Gold e totais de quantidade e valor concluído.

## Verificação e limites

A suíte local verifica dados malformados e nulos, valores negativos, NaN/infinito, limites e estouro decimal, datas impossíveis, câmbio, duplicatas e situações de negócio. Também confere a amostra completa, indicadores duplicados e o caso sem registros aprovados.

Resultado da execução local: **6 testes aprovados**, com PySpark 3.5.1, Python 3.11 e modo ANSI ativo. As contagens da amostra foram confirmadas: Bronze 5.000 = Silver 4.800 + quarentena 200.

Os testes locais **não substituem a execução no Databricks**. Ainda é necessário executar 00 → 04 no workspace para conferir permissões Unity Catalog, volumes, a coluna `_metadata.file_path` e a persistência Delta.

O pipeline é em lote e atualiza as tabelas integralmente. Execute uma vez por vez. Escritas em tabelas diferentes são independentes. O câmbio é fixo e didático. A validação compara contagens e totais, não cada campo de cada linha. CSVs estruturalmente inválidos interrompem a Bronze. Esses limites mantêm o escopo simples e explícito.

## Referências oficiais

- [Limitações da Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).
- [Limitações serverless e modo ANSI](https://docs.databricks.com/aws/en/compute/serverless/limitations).
- [`input_file_name` e Unity Catalog](https://docs.databricks.com/aws/en/sql/language-manual/functions/input_file_name).
- [Metadados dos arquivos](https://docs.databricks.com/aws/en/ingestion/file-metadata-column).
- [Notebooks compartilhados com `%run`](https://docs.databricks.com/aws/en/notebooks/notebook-workflows).
