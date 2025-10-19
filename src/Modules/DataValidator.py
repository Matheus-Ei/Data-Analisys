import pandas as pd


class DataValidator:
    @staticmethod
    def validate(df: pd.DataFrame, required_columns: list[str], data_types: dict[str, str]):
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        for col, dtype in data_types.items():
            if col in df.columns:
                try:
                    if dtype == 'datetime':
                        df[col] = pd.to_datetime(df[col])
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    raise TypeError(f"Could not convert column '{col}' to type '{dtype}': {e}")
        return df

