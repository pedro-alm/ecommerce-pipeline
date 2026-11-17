# Roteiro de apresentação — N3 (17/11)

Tempo sugerido: **10–12 minutos** + perguntas.

## Antes de apresentar (checklist)

- [ ] Rodar `python run_pipeline.py` **inteiro pelo menos uma vez** com o dataset real e conferir que `outputs/relatorio_carga.json` mostra `"confere": true` nas 3 coleções.
- [ ] Commitar a pasta `outputs/` (é a evidência) e o código. **Conferir que `.env` NÃO foi commitado.**
- [ ] Atlas → *Network Access* com o IP do local da apresentação liberado (ou `0.0.0.0/0` só nesse dia).
- [ ] Abrir o Atlas (*Browse Collections*) em uma aba e o terminal já na pasta do projeto, com o venv ativo.
- [ ] Ter o `README.md` aberto no GitHub para mostrar as instruções.
- [ ] **Plano B:** `python run_pipeline.py --amostra` (sem Kaggle) e os arquivos da pasta `outputs/` já gerados.

## Sequência da apresentação

### 1. Problema e dados (1 min)
- Loja online do Reino Unido, ~540 mil linhas de transações (dez/2010 a dez/2011).
- Objetivo: transformar o CSV bruto em dados analíticos consultáveis no MongoDB (receita por mês/país, produtos mais vendidos, clientes, devoluções).

### 2. Arquitetura (2 min) — mostrar o diagrama do README
- Camadas: **RAW** (CSV intocado + hash) → **CURATED** (Parquet particionado por ano/mês) → **agregados** (pedidos, clientes, produtos) → **MongoDB Atlas**.
- Por que PySpark: leitura distribuída, Parquet, escala para volumes maiores sem mudar o código.
- Por que MongoDB: pedido com itens embutidos = um documento por fatura; consultas por data/país/produto com índices.

### 3. Demonstração ao vivo (5 min)
1. `python run_pipeline.py --etapas ingest profile` → mostrar o diagnóstico: nulos, preços zero, cancelamentos, duplicados.
2. `python run_pipeline.py --etapas clean transform` → mostrar `outputs/relatorio_qualidade.json` (quantas linhas lidas × quantas ficaram).
3. `python run_pipeline.py --etapas load` → mostrar o log de upsert e `outputs/relatorio_carga.json` (`confere: true`).
4. **No Atlas:** abrir `pedidos` e mostrar um documento com itens embutidos; mostrar a aba *Indexes*.
5. `python run_pipeline.py --etapas analytics` → mostrar as tabelas no terminal e `outputs/resultado_consultas.md`.
6. (Opcional, impressiona) Rodar `load` **de novo** e mostrar que a contagem não muda: a carga é idempotente (upsert).

> Se a demo ao vivo for lenta, rode só `clean transform` antes e deixe o resultado pronto; execute ao vivo apenas `load` e `analytics`.

### 4. Decisões técnicas e qualidade (1–2 min)
- CSV em **Latin-1** (acentos preservados).
- **Cancelamentos** (`C`) mantidos e sinalizados; receita calculada só sobre pedidos não cancelados.
- **Serviços** (frete, desconto, ajuste) e **preço zero** ficam fora dos agregados, mas permanecem na camada curated com flags.
- Pedidos sem cliente continuam como pedidos; não entram em `clientes`.
- `data_hora` como BSON Date; índices escolhidos conforme as consultas.

### 5. Encerramento (1 min)
- Limitações: execução em lote, Spark local.
- Evoluções: carga incremental agendada, RFM, Atlas Charts.
- Mostrar o repositório no GitHub e a seção "Como executar" do README.

## Perguntas que o professor pode fazer (e respostas curtas)

| Pergunta | Resposta |
|---|---|
| Por que Parquet particionado por ano/mês? | Formato colunar comprimido; consultas por período leem só as partições necessárias. |
| Por que embutir os itens no pedido? | Os itens são sempre lidos junto com o pedido; evita joins (que o MongoDB não faz bem) e mantém atomicidade por fatura. |
| E se rodar o pipeline duas vezes? | Upsert por `_id`: atualiza em vez de duplicar. |
| Como garantem que carregou tudo? | `relatorio_carga.json` compara documentos no Parquet × MongoDB. |
| Como escalaria para bilhões de linhas? | Mesmo código PySpark em cluster; carga em paralelo por partição; sharding no MongoDB. |
| Por que não apagar os dados "sujos"? | Flags preservam rastreabilidade; só os agregados usam dados válidos. |
| Por que pymongo em vez do Spark Connector? | Menos dependências/JARs para instalar e funciona igual no Atlas; o volume (~20 mil pedidos) não exige conector. |

## Checklist de entrega no GitHub

- [ ] Repositório público (ou com acesso ao professor) com o código-fonte.
- [ ] `README.md` com instruções de execução (já incluído).
- [ ] `.env.example` presente; **sem** `.env`, **sem** `data.csv` (o `.gitignore` já cuida disso).
- [ ] Pasta `outputs/` com as saídas da execução real.
- [ ] Link do repositório enviado no prazo (17/11).
