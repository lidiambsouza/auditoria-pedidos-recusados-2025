# src/config/settings.py

import os
import yaml


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Caminhos para os dados de entrada (fontes)
PAGAMENTOS_PATH = os.path.join(PROJECT_ROOT, "dataset", "input", "pagamentos", "*.json.gz")
PEDIDOS_PATH    = os.path.join(PROJECT_ROOT, "dataset", "input", "pedidos", "*.csv.gz")

# Caminho para os dados de saída (destino)
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "dataset", "output")



def carregar_config(path: str = "./auditoria-pedidos-recursados-2025/config/settings.yaml") -> dict:
    """Carrega um arquivo de configuração YAML."""
    with open(path, 'r') as file:
        return yaml.safe_load(file)
