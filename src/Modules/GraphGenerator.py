from pathlib import Path
from Modules.DataLoader import DataLoader
from Modules.DataValidator import DataValidator
from Modules.Plotter import PlotterFactory


class GraphGenerator:
    def __init__(self, data_folder: str = 'data', output_folder: str = 'output'):
        self.data_path = Path(data_folder)
        self.output_path = Path(output_folder)
        self.output_path.mkdir(exist_ok=True)
        self.loader = DataLoader(self.data_path)
        self.validator = DataValidator()

    def generate(self, config: dict):
        file_name = config['file_name']
        plot_type = config['plot_type']
        x_col = config['x_col']
        y_col = config.get('y_col')
        data_types = config.get('data_types', {})
        output_file_name = config['output_file_name']
        delimiter = config.get('delimiter', ',')
        title = config.get('title')
        xlabel = config.get('xlabel')
        ylabel = config.get('ylabel')

        df = self.loader.load_csv(file_name, delimiter=delimiter)

        required_columns = [x_col]
        if y_col:
            required_columns.append(y_col)

        df = self.validator.validate(df, required_columns, data_types)

        plotter = PlotterFactory.get_plotter(plot_type)
        output_file_path = self.output_path / output_file_name

        plotter.plot(df, x_col, y_col, output_file_path, title, xlabel, ylabel)

