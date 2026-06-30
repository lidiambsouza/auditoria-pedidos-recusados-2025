# Análise de Fraude de 2025

Relatório de pedidos de venda cujos pagamentos foram recusados (`status=false`) e que na avaliação de fraude foram classificados como legítimos (`fraude=false`) no período de 2025.

## Tecnologias

- Python 3.13
- PySpark 4.1.1
- PyYAML 6.0.3
- Ruff · Black · Pytest

---

## Estrutura de pastas

```
auditoria-pedidos-recusados-2025/
├── config/
│   └── settings.yaml           # configurações centralizadas (paths, Spark, opções de leitura)
├── dataset/
│   ├── input/
│   │   ├── pagamentos/         # arquivos *.json.gz
│   │   └── pedidos/            # arquivos *.csv.gz
│   └── output/                 # relatório gerado em Parquet
├── src/
│   ├── config/
│   │   └── settings.py         # carrega o YAML e resolve paths absolutos
│   ├── io_utils/
│   │   └── data_handler.py     # leitura e escrita de dados (schemas + validação)
│   ├── pipeline/
│   │   └── pipeline.py         # orquestra o fluxo completo de execução
│   ├── processing/
│   │   └── transformations.py  # regras de negócio e transformações
│   ├── session/
│   │   └── spark_session.py    # criação da SparkSession
│   └── main.py                 # entrypoint, logging
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## Configuração do ambiente

### 1. Criar e ativar o ambiente virtual

```bash
python -m venv venv
source venv/Scripts/activate   # Windows (Git Bash)
source venv/bin/activate        # Linux / Mac
```

### 2. Instalar dependências

```bash
# somente produção
pip install .

# produção + ferramentas de desenvolvimento
pip install ".[dev]"
```

---

## Como executar

### Direto pelo código-fonte

```bash
spark-submit ./src/main.py
```

### Via wheel instalado

**1. Gerar o wheel**

```bash
python -m build
```

Isso cria a pasta `dist/` com o arquivo `analise_de_fraude_2025-1.0.0-py3-none-any.whl`.

**2. Instalar o wheel**

```bash
pip install dist/analise_de_fraude_2025-1.0.0-py3-none-any.whl
```

**3. Executar**

```bash
spark-submit ./src/main.py
```

> Atenção: esteja dentro da pasta raiz do projeto (`auditoria-pedidos-recusados-2025/`) ao rodar o comando, pois o `settings.py` resolve os paths de `dataset/` relativos à raiz do projeto.

---

## Ferramentas de desenvolvimento

```bash
# linting
ruff check .

# formatação
black src/

# testes
pytest

# testes com cobertura
pytest --cov=src --cov-report=term-missing
```

---

## Configuração do ambiente no Windows — Hadoop winutils

### Por que é necessário?

O Spark usa o Hadoop internamente para acessar o sistema de arquivos. No Windows, o Hadoop depende de dois binários nativos para realizar operações como listar diretórios e aplicar padrões glob (`*.json.gz`, `*.csv.gz`):

- `winutils.exe` — utilitário de linha de comando do Hadoop para Windows
- `hadoop.dll` — biblioteca nativa de I/O do Hadoop

Sem esses arquivos, o Spark lança o erro abaixo e não consegue ler os datasets:

```
Did not find winutils.exe: HADOOP_HOME and hadoop.home.dir are unset.
java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(...)'
```

### Passo a passo

**1. Criar a pasta**

```bash
mkdir -p /c/hadoop/bin
```

**2. Baixar os binários**

Acesse o repositório [cdarlint/winutils](https://github.com/cdarlint/winutils) no GitHub e baixe os arquivos da pasta `hadoop-3.3.6/bin/`:

- `winutils.exe`
- `hadoop.dll`

Coloque ambos dentro de `C:\hadoop\bin\`.

> A versão 3.3.6 é compatível com Spark 4.x.

**3. Configurar a variável de ambiente HADOOP_HOME**

No terminal bash — adicione ao `~/.bashrc`:

```bash
export HADOOP_HOME=/c/hadoop
export PATH=$HADOOP_HOME/bin:$PATH
source ~/.bashrc
```

No Windows via PowerShell (permanente):

```powershell
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[System.Environment]::SetEnvironmentVariable("PATH", "C:\hadoop\bin;" + [System.Environment]::GetEnvironmentVariable("PATH","User"), "User")
```

**4. Verificar**

```bash
winutils.exe ls /
```

Se listar o diretório raiz sem erro, está funcionando.

---

## Testes no Windows — leitura via JSON em vez de `createDataFrame`

No Windows, o worker Python do PySpark 4.1.1 crasha (`WinError 10038` ao fechar o socket de comunicação com a JVM) sempre que dados locais precisam voltar pela ponte Python↔JVM — exatamente o que `spark.createDataFrame(lista).collect()` faz:

```
org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
Caused by: java.io.EOFException
```

O problema **não é dos testes nem da versão do Python** (reproduzido em 3.13 e 3.14, fora do pytest). É uma limitação do worker do PySpark 4.x no Windows. Operações nativas da JVM — como `spark.range(...)` e **leitura de arquivos** (`spark.read`) — não passam por esse worker e funcionam normalmente.

Por isso os testes constroem os DataFrames de entrada lendo de um arquivo JSON temporário, em vez de `createDataFrame`:

```python
def make_df(spark, data, schema, tmp_path):
    """Cria um DataFrame a partir de dados locais via arquivo JSON temporário."""
    path = tmp_path / f"{uuid.uuid4().hex}.json"
    with open(path, "w", encoding="utf-8") as f:
        for row in data:
            f.write(json.dumps(_to_json_record(row, schema)) + "\n")
    return spark.read.schema(schema).json(str(path))
```

Esse caminho roda 100% na JVM, preserva todos os tipos (inclusive o struct aninhado `avaliacao_fraude`) e funciona em **qualquer ambiente** (Windows, Linux, Mac, CI). O pipeline de produção nunca foi afetado, pois ele também lê de arquivos (CSV/JSON).

> Em Linux/Mac o `createDataFrame` funciona normalmente; a abordagem via JSON é mantida por ser portável e não ter desvantagem.

---

## Autores

| Nome | E-mail |
|---|---|
| lidiambsouza | lidiambsouza@gmail.com |
| JuliaFQ | queirozjuliadefatima@gmail.com |
| VictorManks | victorfdefariaq@gmail.com |
