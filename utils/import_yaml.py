import yaml

def import_yaml(filename: str) -> dict:
    """
    Importer un fichier YAML

    Args:
        filename (str): Emplacement du fichier

    Returns:
        dict: Le fichier YAML sous forme de dictionnaire Python
    """
    with open(filename, "r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
        return config

import_yaml("config.yaml")