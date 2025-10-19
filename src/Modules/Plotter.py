from abc import ABC, abstractmethod
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional


class Plotter(ABC):
    @abstractmethod
    def plot(self, df: pd.DataFrame, x_col: str, y_col: Optional[str], output_path: Path):
        pass


class BarPlotter(Plotter):
    def plot(self, df: pd.DataFrame, x_col: str, y_col: Optional[str], output_path: Path):
        plt.figure(figsize=(10, 6))
        sns.barplot(data=df, x=x_col, y=y_col)
        plt.title(f'Bar Plot of {y_col} vs {x_col}')
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class LinePlotter(Plotter):
    def plot(self, df: pd.DataFrame, x_col: str, y_col: Optional[str], output_path: Path):
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=df, x=x_col, y=y_col, marker='o')
        plt.title(f'Line Plot of {y_col} vs {x_col}')
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class ScatterPlotter(Plotter):
    def plot(self, df: pd.DataFrame, x_col: str, y_col: Optional[str], output_path: Path):
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x=x_col, y=y_col)
        plt.title(f'Scatter Plot of {y_col} vs {x_col}')
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class HistogramPlotter(Plotter):
    def plot(self, df: pd.DataFrame, x_col: str, y_col: Optional[str], output_path: Path):
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df, x=x_col, kde=True)
        plt.title(f'Histogram of {x_col}')
        plt.xlabel(x_col)
        plt.ylabel('Frequency')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class PlotterFactory:
    plotters = {
        'bar': BarPlotter,
        'line': LinePlotter,
        'scatter': ScatterPlotter,
        'histogram': HistogramPlotter
    }

    @staticmethod
    def get_plotter(plot_type: str) -> Plotter:
        plotter_class = PlotterFactory.plotters.get(plot_type.lower())
        if not plotter_class:
            raise ValueError(f"Unknown plot type: {plot_type}. Available types: {list(PlotterFactory.plotters.keys())}")
        return plotter_class()

