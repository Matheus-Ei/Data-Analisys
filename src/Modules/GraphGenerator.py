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
        output_file_name = config['output_file_name']
        delimiter = config.get('delimiter', ',')
        data_types = config.get('data_types', {})

        df = self.loader.load_csv(file_name, delimiter=delimiter)

        required_columns = []
        if 'x_col' in config:
            required_columns.append(config['x_col'])
        if 'y_col' in config:
            required_columns.append(config['y_col'])
        if 'columns' in config:
            required_columns.extend(config['columns'])

        if required_columns:
            df = self.validator.validate(df, list(set(required_columns)), data_types)

        plotter = PlotterFactory.get_plotter(plot_type)
        output_file_path = self.output_path / output_file_name

        plot_kwargs = config.copy()

        plotter.plot(df, output_file_path, **plot_kwargs)
