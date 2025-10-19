import pandas as pd
from typing import Any
from sklearn.preprocessing import MinMaxScaler


class DataProcessor:
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe.copy()

    def rename_columns(self, column_map: dict[str, str]):
        self.df.rename(columns=column_map, inplace=True)
        return self

    def filter_rows(self, column: str, operator: str, value: Any):
        query_string = f"`{column}` {operator} {repr(value)}"
        self.df = self.df.query(query_string).copy()
        return self

    def convert_type(self, column: str, new_type: str):
        self.df[column] = self.df[column].astype(new_type)
        return self

    def impute_by_group_mean(self, target_col: str, group_by_col: str):
        group_means = self.df.groupby(group_by_col)[target_col].transform('mean')
        self.df[target_col] = self.df[target_col].fillna(group_means)
        self.df[target_col] = self.df[target_col].fillna(self.df[target_col].mean())
        return self

    def normalize_column(self, column: str):
        scaler = MinMaxScaler()
        self.df[column] = scaler.fit_transform(self.df[[column]])
        return self

    def map_columns(self, column_maps: dict[str, dict]):
        for col, mapping in column_maps.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].map(mapping)
        return self

    def add_duration_column(self, result_col: str, start_col: str, end_col: str):
        self.df[result_col] = self.df[end_col] - self.df[start_col]
        return self

    def remove_outliers_by_group(self, data_col: str, group_by_col: str):
        Q1 = self.df.groupby(group_by_col)[data_col].transform('quantile', 0.25)
        Q3 = self.df.groupby(group_by_col)[data_col].transform('quantile', 0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        mask = (self.df[data_col] >= lower_bound) & (self.df[data_col] <= upper_bound)
        self.df = self.df[mask].copy()

        return self

    def get_robust_group_aggregation(self, group_by_col: str, agg_col: str, min_sample_size: int = 30) -> pd.DataFrame:
        counts = self.df[group_by_col].value_counts()
        significant_groups = counts[counts >= min_sample_size].index

        if significant_groups.empty:
            print(f"--> WARNING: No groups in '{group_by_col}' met the minimum sample size of {min_sample_size}. The resulting aggregation will be empty.")
            return pd.DataFrame(columns=[group_by_col, agg_col])

        df_filtered = self.df[self.df[group_by_col].isin(significant_groups)].copy()

        Q1 = df_filtered.groupby(group_by_col)[agg_col].transform('quantile', 0.25)
        Q3 = df_filtered.groupby(group_by_col)[agg_col].transform('quantile', 0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        mask = (df_filtered[agg_col] >= lower_bound) & (df_filtered[agg_col] <= upper_bound)
        df_no_outliers = df_filtered[mask]

        aggregated_df = df_no_outliers.groupby(group_by_col)[agg_col].mean().reset_index()

        return aggregated_df

    def build(self) -> pd.DataFrame:
        return self.df.copy()
