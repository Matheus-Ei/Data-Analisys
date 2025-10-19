import pandas as pd
from typing import Dict, Any


class DataTransformer:
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe.copy()

    def map_columns(self, column_maps: Dict[str, Dict[Any, str]]):
        for column, mapping in column_maps.items():
            if column in self.df.columns:
                self.df[column] = self.df[column].map(mapping)
        return self

    def build(self) -> pd.DataFrame:
        return self.df.copy()
