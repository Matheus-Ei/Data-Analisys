import pandas as pd
from pathlib import Path
from typing import Any, Optional
from sklearn.preprocessing import MinMaxScaler


class DataProcessor:
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe.copy()

    def handle_missing_values(self, column: str, strategy: str = 'mean', fill_value: Optional[Any] = None):
        if strategy == 'mean':
            fill_value = self.df[column].mean()
        elif strategy == 'median':
            fill_value = self.df[column].median()
        elif strategy == 'mode':
            fill_value = self.df[column].mode()[0]
        elif strategy == 'constant' and fill_value is None:
            raise ValueError("A fill_value must be provided for the 'constant' strategy.")

        self.df[column].fillna(fill_value, inplace=True)
        return self

    def normalize_column(self, column: str):
        scaler = MinMaxScaler()
        self.df[column] = scaler.fit_transform(self.df[[column]])
        return self

    def filter_rows(self, column: str, operator: str, value: Any):
        query_string = f"`{column}` {operator} {repr(value)}"
        self.df = self.df.query(query_string)
        return self

    def rename_columns(self, column_map: dict[str, str]):
        self.df.rename(columns=column_map, inplace=True)
        return self

    def convert_type(self, column: str, new_type: str):
        if new_type == 'datetime':
            self.df[column] = pd.to_datetime(self.df[column])
        else:
            self.df[column] = self.df[column].astype(new_type)
        return self

    def build(self) -> pd.DataFrame:
        return self.df.copy()

    def save_to_csv(self, output_path: Path, **kwargs):
        processed_df = self.build()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        processed_df.to_csv(output_path, index=False, **kwargs)

