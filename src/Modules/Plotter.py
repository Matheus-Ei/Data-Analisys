from abc import ABC, abstractmethod
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional, List
import numpy as np


class Plotter(ABC):
    @abstractmethod
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        pass


class BarPlotter(Plotter):
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        x_col = kwargs.get('x_col')
        y_col = kwargs.get('y_col')
        title = kwargs.get('title')
        xlabel = kwargs.get('xlabel')
        ylabel = kwargs.get('ylabel')

        plt.figure(figsize=(12, 8))
        sns.barplot(data=df, x=x_col, y=y_col)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class LinePlotter(Plotter):
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        x_col = kwargs.get('x_col')
        y_col = kwargs.get('y_col')
        title = kwargs.get('title')
        xlabel = kwargs.get('xlabel')
        ylabel = kwargs.get('ylabel')

        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df, x=x_col, y=y_col, marker='o')
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class ScatterPlotter(Plotter):
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        x_col = kwargs.get('x_col')
        y_col = kwargs.get('y_col')
        title = kwargs.get('title')
        xlabel = kwargs.get('xlabel')
        ylabel = kwargs.get('ylabel')
        hue = kwargs.get('hue')

        plt.figure(figsize=(12, 8))
        sns.scatterplot(data=df, x=x_col, y=y_col, hue=hue, alpha=0.6)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class HistogramPlotter(Plotter):
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        x_col = kwargs.get('x_col')
        title = kwargs.get('title')
        xlabel = kwargs.get('xlabel')
        ylabel = kwargs.get('ylabel')

        plt.figure(figsize=(10, 6))
        sns.histplot(data=df, x=x_col, kde=True)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel if ylabel else 'Frequency')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class BoxPlotter(Plotter):
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        x_col = kwargs.get('x_col')
        y_col = kwargs.get('y_col')
        title = kwargs.get('title')
        xlabel = kwargs.get('xlabel')
        ylabel = kwargs.get('ylabel')

        plt.figure(figsize=(14, 8))
        sns.boxplot(data=df, x=x_col, y=y_col)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class ViolinPlotter(Plotter):
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        x_col = kwargs.get('x_col')
        y_col = kwargs.get('y_col')
        title = kwargs.get('title')
        xlabel = kwargs.get('xlabel')
        ylabel = kwargs.get('ylabel')

        plt.figure(figsize=(14, 8))
        sns.violinplot(data=df, x=x_col, y=y_col)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class HeatmapPlotter(Plotter):
    def plot(self, df: pd.DataFrame, output_path: Path, **kwargs):
        title = kwargs.get('title')
        columns = kwargs.get('columns')

        plt.figure(figsize=(12, 10))

        if columns:
            data_to_correlate = df[columns]
        else:
            data_to_correlate = df.select_dtypes(include=np.number)

        corr = data_to_correlate.corr()

        sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f", annot_kws={"size": 10})
        plt.title(title, fontsize=16)
        plt.xticks(rotation=45, ha='right')
        plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


class PlotterFactory:
    plotters = {
        'bar': BarPlotter,
        'line': LinePlotter,
        'scatter': ScatterPlotter,
        'histogram': HistogramPlotter,
        'box': BoxPlotter,
        'violin': ViolinPlotter,
        'heatmap': HeatmapPlotter,
    }

    @staticmethod
    def get_plotter(plot_type: str) -> Plotter:
        plotter_class = PlotterFactory.plotters.get(plot_type.lower())
        if not plotter_class:
            raise ValueError(f"Unknown plot type: {plot_type}. Available types: {list(PlotterFactory.plotters.keys())}")
        return plotter_class()

