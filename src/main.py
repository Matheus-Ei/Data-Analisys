from pathlib import Path
import numpy as np
from Modules.GraphGenerator import GraphGenerator
from Modules.DataLoader import DataLoader
from Modules.DataProcessor import DataProcessor
from Modules.DataTransformer import DataTransformer


def main():
    data_folder = Path('data')
    raw_data_file = '2022.txt'
    processed_data_file = '2021_processed.csv'
    admin_performance_file = 'admin_category_performance.csv'
    modality_performance_file = 'modality_performance.csv'

    loader = DataLoader(data_folder)
    raw_df = loader.load_csv(raw_data_file, delimiter=';')

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
        .handle_missing_values('ENEM_NT_CH', strategy='constant', fill_value=np.nan) \
        .handle_missing_values('ENEM_NT_LC', strategy='constant', fill_value=np.nan) \
        .handle_missing_values('ENEM_NT_MT', strategy='constant', fill_value=np.nan) \
        .normalize_column('NT_GER') \
        .convert_type('ANO_IN_GRAD', 'int') \
        .rename_columns(rename_dict)

    processed_df = processor.build()
    
    column_maps = {
        'CODIGO_CATEGORIA_ADMINISTRATIVA': {
            1: 'Pública Federal', 2: 'Pública Estadual', 3: 'Pública Municipal',
            4: 'Privada com fins lucrativos', 5: 'Privada sem fins lucrativos', 7: 'Especial'
        },
        'CODIGO_ORGANIZACAO_ACADEMICA': {
            10019: 'Centro Federal de Educação Tecnológica', 10020: 'Centro Universitário',
            10022: 'Faculdade', 10026: 'Instituto Federal de Educação, Ciência e Tecnologia',
            10028: 'Universidade'
        },
        'CODIGO_AREA_AVALIACAO': {
            26: 'Design', 72: 'Tecnologia em Análise e Desenvolvimento de Sistemas',
            79: 'Tecnologia em Redes de Computadores', 702: 'Matemática (Licenciatura)',
            904: 'Letras-Português (Licenciatura)', 905: 'Letras-Português e Inglês (Licenciatura)',
            906: 'Letras-Português e Espanhol (Licenciatura)', 1402: 'Física (Licenciatura)',
            1602: 'Ciências Biológicas (Licenciatura)', 2001: 'Educação Física (Licenciatura)',
            2202: 'Pedagogia (Licenciatura)', 2401: 'História (Bacharelado)',
            2402: 'História (Licenciatura)', 2501: 'Artes Visuais (Licenciatura)',
            3001: 'Geografia (Bacharelado)', 3002: 'Geografia (Licenciatura)',
            3201: 'Filosofia (Bacharelado)', 3202: 'Filosofia (Licenciatura)',
            3502: 'Química (Licenciatura)', 4003: 'Sistemas de Informação',
            4301: 'Música (Licenciatura)', 5401: 'Ciências Sociais (Bacharelado)',
            5402: 'Ciências Sociais (Licenciatura)', 6407: 'Letras-Inglês (Licenciatura)',
            6409: 'Tecnologia em Gestão da Tecnologia da Informação'
        },
        'CODIGO_MODALIDADE_ENSINO': {0: 'EaD', 1: 'Presencial'},
        'TIPO_INSCRICAO_ENADE': {1: 'Concluinte'},
        'INDICADOR_SITUACAO_REGULAR': {1: 'Regular'},
        'TIPO_INSCRICAO_ADMINISTRATIVA': {0: 'Tradicional', 2: 'Administrativo'},
        'TIPO_PRESENCA_PROVA': {555: 'Presente com resultado válido'}
    }

    transformer = DataTransformer(processed_df)
    transformed_df = transformer.map_columns(column_maps).build()

    transformed_df['DURATION_GRADUATION'] = transformed_df['ANO_ENADE'] - transformed_df['ANO_INICIO_GRADUACAO']
    transformed_df.to_csv(data_folder / processed_data_file, index=False)

    admin_performance_df = transformed_df.groupby('CODIGO_CATEGORIA_ADMINISTRATIVA')['NOTA_GERAL_ENADE'].mean().reset_index()
    admin_performance_df.to_csv(data_folder / admin_performance_file, index=False)

    modality_performance_df = transformed_df.groupby('CODIGO_MODALIDADE_ENSINO')['NOTA_GERAL_ENADE'].mean().reset_index()
    modality_performance_df.to_csv(data_folder / modality_performance_file, index=False)

    generator = GraphGenerator(data_folder='data', output_folder='output')

    graph_configs = [
        {
            'file_name': processed_data_file, 'plot_type': 'histogram',
            'x_col': 'NOTA_GERAL_ENADE', 'output_file_name': 'enade_grades_distribution.png',
            'title': 'Distribution of ENADE Grades', 'xlabel': 'ENADE Grade (Normalized)', 'ylabel': 'Frequency'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'histogram',
            'x_col': 'NOTA_ENEM_CIENCIAS_NATUREZA', 'output_file_name': 'enem_science_grades_distribution.png',
            'title': 'Distribution of ENEM Science Grades', 'xlabel': 'ENEM Science Grade', 'ylabel': 'Frequency'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'histogram',
            'x_col': 'NOTA_ENEM_CIENCIAS_HUMANAS', 'output_file_name': 'enem_humanities_grades_distribution.png',
            'title': 'Distribution of ENEM Humanities Grades', 'xlabel': 'ENEM Humanities Grade', 'ylabel': 'Frequency'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'histogram',
            'x_col': 'NOTA_ENEM_LINGUAGENS_CODIGOS', 'output_file_name': 'enem_languages_grades_distribution.png',
            'title': 'Distribution of ENEM Languages Grades', 'xlabel': 'ENEM Languages Grade', 'ylabel': 'Frequency'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'histogram',
            'x_col': 'NOTA_ENEM_MATEMATICA', 'output_file_name': 'enem_math_grades_distribution.png',
            'title': 'Distribution of ENEM Math Grades', 'xlabel': 'ENEM Math Grade', 'ylabel': 'Frequency'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'scatter',
            'x_col': 'NOTA_ENEM_MATEMATICA', 'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'scatter_enade_vs_enem_math.png', 'title': 'ENADE Grade vs. ENEM Math Grade',
            'xlabel': 'ENEM Math Grade', 'ylabel': 'ENADE Grade (Normalized)'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'scatter',
            'x_col': 'NOTA_ENEM_CIENCIAS_NATUREZA', 'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'scatter_enade_vs_enem_science.png', 'title': 'ENADE Grade vs. ENEM Science Grade',
            'xlabel': 'ENEM Science Grade', 'ylabel': 'ENADE Grade (Normalized)'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'scatter',
            'x_col': 'NOTA_ENEM_CIENCIAS_HUMANAS', 'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'scatter_enade_vs_enem_humanities.png', 'title': 'ENADE Grade vs. ENEM Humanities Grade',
            'xlabel': 'ENEM Humanities Grade', 'ylabel': 'ENADE Grade (Normalized)'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'scatter',
            'x_col': 'NOTA_ENEM_LINGUAGENS_CODIGOS', 'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'scatter_enade_vs_enem_languages.png', 'title': 'ENADE Grade vs. ENEM Languages Grade',
            'xlabel': 'ENEM Languages Grade', 'ylabel': 'ENADE Grade (Normalized)'
        },
        {
            'file_name': processed_data_file, 'plot_type': 'scatter',
            'x_col': 'DURATION_GRADUATION', 'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'scatter_enade_vs_graduation_duration.png', 'title': 'ENADE Grade vs. Graduation Duration',
            'xlabel': 'Graduation Duration (Years)', 'ylabel': 'ENADE Grade (Normalized)'
        },
        {
            'file_name': admin_performance_file, 'plot_type': 'bar',
            'x_col': 'CODIGO_CATEGORIA_ADMINISTRATIVA', 'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'bar_performance_by_admin_category.png', 'title': 'Average ENADE Grade by Administrative Category',
            'xlabel': 'Administrative Category', 'ylabel': 'Average ENADE Grade (Normalized)'
        },
        {
            'file_name': modality_performance_file, 'plot_type': 'bar',
            'x_col': 'CODIGO_MODALIDADE_ENSINO', 'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'bar_performance_by_modality.png', 'title': 'Average ENADE Grade by Teaching Modality',
            'xlabel': 'Teaching Modality', 'ylabel': 'Average ENADE Grade (Normalized)'
        }
    ]

    for config in graph_configs:
        try:
            generator.generate(config)
        except Exception as e:
            print(f"Failed to generate graph {config['output_file_name']}: {e}")


if __name__ == "__main__":
    main()
