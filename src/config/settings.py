# src/config/settings.py

import os
import yaml

# cross-platform
# os.path.abspath garante o caminho absoluto do arquivo atual independente de onde o script é chamado.
# os.path.dirname sobe um nível por vez: settings.py → src/config/ → src/ → raiz do projeto.
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


class Config:
    """Carrega e centraliza as configurações da aplicação a partir do settings.yaml."""

    def __init__(self, path: str = None):
        self._path = path or os.path.join(PROJECT_ROOT, "config", "settings.yaml")
        self._data = self._carregar()

    def _carregar(self) -> dict:
        with open(self._path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        # O YAML armazena caminhos com "/" (padrão Unix). O split("/") separa os segmentos e
        # o os.path.join os remonta usando o separador correto de cada SO ("\" no Windows, "/" no Linux/Mac),
        # tornando os caminhos compatíveis sem nenhuma alteração de código entre ambientes.
        for key, value in config.get("paths", {}).items():
            config["paths"][key] = os.path.join(PROJECT_ROOT, *value.split("/"))
        return config

    @property
    def data(self) -> dict:
        """Retorna as configurações carregadas como dicionário."""
        return self._data
