# auditoria-pedidos-recusados-2025
Relatório de pedidos de venda cujo pagamentos recusados (status=false) e que na avaliação de fraude foram classificados como legítimos (fraude=false) de 2025

## Estrutura de pastas

```
auditoria-pedidos-recusados-2025/
├── src/
│   ├── main.py
│   └── dataset/
│       ├── input/
│       │   ├── pagamentos/   # arquivos *.json.gz
│       │   └── pedidos/      # arquivos *.csv.gz
│       └── output/
├── requirements.txt
└── README.md
```

## Como executar

```bash
spark-submit ./src/main.py
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

Ou pelo PowerShell/Prompt:

```
mkdir C:\hadoop\bin
```

**2. Baixar os binários**

Acesse o repositório [cdarlint/winutils](https://github.com/cdarlint/winutils) no GitHub e baixe os arquivos da pasta `hadoop-3.3.6/bin/`:

- `winutils.exe`
- `hadoop.dll`

Coloque ambos dentro de `C:\hadoop\bin\`.

> A versão 3.3.6 é compatível com Spark 4.x.

**3. Configurar a variável de ambiente HADOOP_HOME**

**No terminal bash (Git Bash)** — adicione ao `~/.bashrc`:

```bash
export HADOOP_HOME=/c/hadoop
export PATH=$HADOOP_HOME/bin:$PATH
```

Depois aplique:

```bash
source ~/.bashrc
```

**No Windows (permanente via PowerShell)**:

```powershell
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[System.Environment]::SetEnvironmentVariable("PATH", "C:\hadoop\bin;" + [System.Environment]::GetEnvironmentVariable("PATH","User"), "User")
```

**4. Verificar**

```bash
winutils.exe ls /
```

Se listar o diretório raiz sem erro, está funcionando.

**5. Executar o projeto**

```bash
spark-submit ./src/main.py
```
