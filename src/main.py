from pathlib import Path
import numpy as np
from Modules.GraphGenerator import GraphGenerator
from Modules.DataLoader import DataLoader
from Modules.DataProcessor import DataProcessor


def main():
    # Load raw data
    data_folder = Path('data')
    raw_data_file = '2021.txt'
    processed_data_file = '2021_processed.csv'

    loader = DataLoader(data_folder)
    raw_df = loader.load_csv(raw_data_file, delimiter=';')

    # Process raw data
    processor = DataProcessor(raw_df)

    rename_dict = {
        'NU_ANO': 'ANO_ENADE',
        'CO_IES': 'CODIGO_IES',
        'CO_CATEGAD': 'CODIGO_CATEGORIA_ADMINISTRATIVA',
        'CO_ORGACAD': 'CODIGO_ORGANIZACAO_ACADEMICA',
        'CO_GRUPO': 'CODIGO_AREA_AVALIACAO',
        'CO_CURSO': 'CODIGO_CURSO',
        'CO_MODALIDADE': 'CODIGO_MODALIDADE_ENSINO',
        'CO_MUNIC_CURSO': 'CODIGO_MUNICIPIO_CURSO',
        'TP_INSCRICAO': 'TIPO_INSCRICAO_ENADE',
        'IN_REGULAR': 'INDICADOR_SITUACAO_REGULAR',
        'TP_INSCRICAO_ADM': 'TIPO_INSCRICAO_ADMINISTRATIVA',
        'ANO_IN_GRAD': 'ANO_INICIO_GRADUACAO',
        'TP_PRES': 'TIPO_PRESENCA_PROVA',
        'NT_GER': 'NOTA_GERAL_ENADE',
        'ANO_ENEM': 'ANO_REALIZACAO_ENEM',
        'ENEM_NT_CN': 'NOTA_ENEM_CIENCIAS_NATUREZA',
        'ENEM_NT_CH': 'NOTA_ENEM_CIENCIAS_HUMANAS',
        'ENEM_NT_LC': 'NOTA_ENEM_LINGUAGENS_CODIGOS',
        'ENEM_NT_MT': 'NOTA_ENEM_MATEMATICA'
    }

    processor.filter_rows('NT_GER', '>', 0) \
             .handle_missing_values('ENEM_NT_CN', strategy='constant', fill_value=np.nan) \
             .handle_missing_values('ENEM_NT_MT', strategy='constant', fill_value=np.nan) \
             .normalize_column('NT_GER') \
             .convert_type('ANO_IN_GRAD', 'int') \
             .rename_columns(rename_dict)

    processed_file_path = data_folder / processed_data_file
    processor.save_to_csv(processed_file_path)

    # Generate graphs
    generator = GraphGenerator(data_folder='data', output_folder='output')
    graph_configs = [
        {
            'file_name': processed_data_file,
            'plot_type': 'histogram',
            'x_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'grades_distribution.png'
        },
        {
            'file_name': processed_data_file,
            'plot_type': 'scatter',
            'x_col': 'NOTA_ENEM_MATEMATICA',
            'y_col': 'NOTA_ENEM_CIENCIAS_NATUREZA',
            'output_file_name': 'math_vs_science.png'
        }
    ]

    for config in graph_configs:
        try:
            generator.generate(config)
        except Exception as e:
            print(f"Could not generate graph for config {config.get('output_file_name', 'N/A')}: {e}")


if __name__ == "__main__":
    main()

