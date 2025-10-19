import pandas as pd
from pathlib import Path


class DataLoader:
    def __init__(self, data_folder: Path):
        self.data_folder = data_folder
        if not self.data_folder.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_folder}")

    def load_csv(self, file_name: str, delimiter: str = ',') -> pd.DataFrame:
        file_path = self.data_folder / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        return pd.read_csv(file_path, delimiter=delimiter)

