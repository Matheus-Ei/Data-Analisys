import shutil
from pathlib import Path
from Modules.GraphGenerator import GraphGenerator
from Modules.DataLoader import DataLoader
from Modules.DataProcessor import DataProcessor


def clear_directory(directory_path: Path):
    if directory_path.exists():
        for item in directory_path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


def main():
    # --- Configuration ---
    base_data_folder = Path('data')
    raw_data_folder = base_data_folder / 'raw'
    processed_data_folder = base_data_folder / 'processed'
    output_folder = Path('output')

    # --- Clean up previous runs ---
    clear_directory(processed_data_folder)
    clear_directory(output_folder)

    processed_data_folder.mkdir(parents=True, exist_ok=True)
    output_folder.mkdir(parents=True, exist_ok=True)

    raw_data_file = '2021.txt'
    processed_data_file = '2021_processed.csv'
    admin_performance_file = 'admin_category_performance.csv'
    modality_performance_file = 'modality_performance.csv'
    area_performance_file = 'area_performance.csv'
    top_5_areas_file = 'top_5_areas.csv'
    bottom_5_areas_file = 'bottom_5_areas.csv'
    performance_by_year_file = 'performance_by_year.csv'
    MIN_SAMPLE_SIZE = 30

    # --- Step 1: Processing, Transformation, and Saving ---
    loader = DataLoader(raw_data_folder)
    raw_df = loader.load_csv(raw_data_file, delimiter=';')

    processor = DataProcessor(raw_df)

    rename_dict = {
        'NU_ANO': 'ANO_ENADE', 'CO_IES': 'CODIGO_IES', 'CO_CATEGAD': 'CODIGO_CATEGORIA_ADMINISTRATIVA',
        'CO_ORGACAD': 'CODIGO_ORGANIZACAO_ACADEMICA', 'CO_GRUPO': 'CODIGO_AREA_AVALIACAO',
        'CO_CURSO': 'CODIGO_CURSO', 'CO_MODALIDADE': 'CODIGO_MODALIDADE_ENSINO',
        'CO_MUNIC_CURSO': 'CODIGO_MUNICIPIO_CURSO', 'TP_INSCRICAO': 'TIPO_INSCRICAO_ENADE',
        'IN_REGULAR': 'INDICADOR_SITUACAO_REGULAR', 'TP_INSCRICAO_ADM': 'TIPO_INSCRICAO_ADMINISTRATIVA',
        'ANO_IN_GRAD': 'ANO_INICIO_GRADUACAO', 'TP_PRES': 'TIPO_PRESENCA_PROVA', 'NT_GER': 'NOTA_GERAL_ENADE',
        'ANO_ENEM': 'ANO_REALIZACAO_ENEM', 'ENEM_NT_CN': 'NOTA_ENEM_CIENCIAS_NATUREZA',
        'ENEM_NT_CH': 'NOTA_ENEM_CIENCIAS_HUMANAS', 'ENEM_NT_LC': 'NOTA_ENEM_LINGUAGENS_CODIGOS',
        'ENEM_NT_MT': 'NOTA_ENEM_MATEMATICA'
    }

    column_maps = {
        'CODIGO_CATEGORIA_ADMINISTRATIVA': {
            1: 'Pública Federal', 2: 'Pública Estadual', 3: 'Pública Municipal',
            4: 'Privada com fins lucrativos', 5: 'Privada sem fins lucrativos', 7: 'Especial'
        },

        'CODIGO_ORGANIZACAO_ACADEMICA': {
            10019: 'Centro Federal de Educação Tecnológica', 10020: 'Centro Universitário',
            10022: 'Faculdade', 10026: 'Inst. Federal de Educação, Ciência e Tec.', 10028: 'Universidade'
        },

        'CODIGO_AREA_AVALIACAO': {
            26: 'Design', 72: 'Tec. ADS', 79: 'Tec. Redes', 702: 'Matemática (Lic)', 904: 'Letras-Português',
            905: 'Letras-Port./Inglês', 906: 'Letras-Port./Espanhol', 1402: 'Física (Lic)',
            1602: 'Biologia (Lic)', 2001: 'Ed. Física (Lic)', 2202: 'Pedagogia', 2401: 'História (Bach)',
            2402: 'História (Lic)', 2501: 'Artes Visuais (Lic)', 3001: 'Geografia (Bach)', 3002: 'Geografia (Lic)',
            3201: 'Filosofia (Bach)', 3202: 'Filosofia (Lic)', 3502: 'Química (Lic)', 4003: 'Sistemas de Informação',
            4301: 'Música (Lic)', 5401: 'Ciências Sociais (Bach)', 5402: 'Ciências Sociais (Lic)',
            6407: 'Letras-Inglês', 6409: 'Tec. Gestão de TI'
        },

        'CODIGO_MODALIDADE_ENSINO': {0: 'EaD', 1: 'Presencial'},
    }

    processor.filter_rows('NT_GER', '>', 0) \
       .rename_columns(rename_dict) \
       .impute_by_group_mean('NOTA_ENEM_CIENCIAS_NATUREZA', 'CODIGO_CATEGORIA_ADMINISTRATIVA') \
       .impute_by_group_mean('NOTA_ENEM_CIENCIAS_HUMANAS', 'CODIGO_CATEGORIA_ADMINISTRATIVA') \
       .impute_by_group_mean('NOTA_ENEM_LINGUAGENS_CODIGOS', 'CODIGO_CATEGORIA_ADMINISTRATIVA') \
       .impute_by_group_mean('NOTA_ENEM_MATEMATICA', 'CODIGO_CATEGORIA_ADMINISTRATIVA') \
       .remove_outliers_by_group('NOTA_ENEM_MATEMATICA', 'CODIGO_CATEGORIA_ADMINISTRATIVA') \
       .remove_outliers_by_group('NOTA_GERAL_ENADE', 'CODIGO_CATEGORIA_ADMINISTRATIVA') \
       .normalize_column('NOTA_GERAL_ENADE') \
       .convert_type('ANO_INICIO_GRADUACAO', 'int') \
       .map_columns(column_maps) \
       .add_duration_column('DURATION_GRADUATION', 'ANO_INICIO_GRADUACAO', 'ANO_ENADE')

    processed_df = processor.build()
    processed_df.to_csv(processed_data_folder / processed_data_file, index=False)

    # --- Step 2: Aggregation for Analysis ---
    admin_performance_df = processor.get_robust_group_aggregation('CODIGO_CATEGORIA_ADMINISTRATIVA', agg_col='NOTA_GERAL_ENADE', min_sample_size=MIN_SAMPLE_SIZE)
    modality_performance_df = processor.get_robust_group_aggregation('CODIGO_MODALIDADE_ENSINO', agg_col='NOTA_GERAL_ENADE', min_sample_size=MIN_SAMPLE_SIZE)
    area_performance_df = processor.get_robust_group_aggregation('CODIGO_AREA_AVALIACAO', agg_col='NOTA_GERAL_ENADE', min_sample_size=MIN_SAMPLE_SIZE).sort_values(by='NOTA_GERAL_ENADE', ascending=False)
    performance_by_year_df = processor.get_robust_group_aggregation('ANO_INICIO_GRADUACAO', agg_col='NOTA_GERAL_ENADE', min_sample_size=MIN_SAMPLE_SIZE)

    admin_performance_df.to_csv(processed_data_folder / admin_performance_file, index=False)
    modality_performance_df.to_csv(processed_data_folder / modality_performance_file, index=False)
    area_performance_df.to_csv(processed_data_folder / area_performance_file, index=False)
    performance_by_year_df.to_csv(processed_data_folder / performance_by_year_file, index=False)

    if not area_performance_df.empty:
        top_5_areas_df = area_performance_df.head(5)
        bottom_5_areas_df = area_performance_df.tail(5).sort_values(by='NOTA_GERAL_ENADE', ascending=True)
        top_5_areas_df.to_csv(processed_data_folder / top_5_areas_file, index=False)
        bottom_5_areas_df.to_csv(processed_data_folder / bottom_5_areas_file, index=False)

    # --- Step 3: Graph Generation ---
    generator = GraphGenerator(data_folder=processed_data_folder, output_folder=output_folder)

    graph_configs = [
        {
            'file_name': processed_data_file,
            'plot_type': 'heatmap',
            'output_file_name': 'enem_correlation_heatmap.png',
            'title': 'Correlation Heatmap of ENEM Grades',
            'columns': ['NOTA_ENEM_CIENCIAS_NATUREZA', 'NOTA_ENEM_CIENCIAS_HUMANAS', 'NOTA_ENEM_LINGUAGENS_CODIGOS', 'NOTA_ENEM_MATEMATICA']
        },

        {
            'file_name': processed_data_file,
            'plot_type': 'heatmap',
            'output_file_name': 'enade_correlation_heatmap.png',
            'title': 'Correlation of ENADE Grade and Graduation Duration',
            'columns': ['NOTA_GERAL_ENADE', 'DURATION_GRADUATION']
        },

        {
            'file_name': processed_data_file,
            'plot_type': 'histogram',
            'x_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'enade_grades_distribution.png',
            'title': 'Distribution of ENADE Grades',
            'xlabel': 'ENADE Grade (Normalized)',
            'ylabel': 'Frequency'
        },

        {
            'file_name': processed_data_file,
            'plot_type': 'histogram',
            'x_col': 'DURATION_GRADUATION',
            'output_file_name': 'graduation_duration_distribution.png',
            'title': 'Distribution of Graduation Duration',
            'xlabel': 'Duration in Years',
            'ylabel': 'Frequency'
        },

        {
            'file_name': admin_performance_file,
            'plot_type': 'bar',
            'x_col': 'CODIGO_CATEGORIA_ADMINISTRATIVA',
            'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'bar_performance_by_admin_category.png',
            'title': 'Average ENADE Grade by Administrative Category',
            'xlabel': 'Administrative Category',
            'ylabel': 'Average ENADE Grade (Normalized)'
        },

        {
            'file_name': top_5_areas_file,
            'plot_type': 'bar',
            'x_col': 'CODIGO_AREA_AVALIACAO',
            'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'bar_top_5_areas.png',
            'title': 'Top 5 Evaluation Areas by Average ENADE Grade',
            'xlabel': 'Evaluation Area',
            'ylabel': 'Average ENADE Grade (Normalized)'
        },

        {
            'file_name': bottom_5_areas_file,
            'plot_type': 'bar',
            'x_col': 'CODIGO_AREA_AVALIACAO',
            'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'bar_bottom_5_areas.png',
            'title': 'Bottom 5 Evaluation Areas by Average ENADE Grade',
            'xlabel': 'Evaluation Area',
            'ylabel': 'Average ENADE Grade (Normalized)'
        },

        {
            'file_name': performance_by_year_file,
            'plot_type': 'line',
            'x_col': 'ANO_INICIO_GRADUACAO',
            'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'line_performance_by_start_year.png',
            'title': 'Average ENADE Grade by Graduation Start Year (Robust)',
            'xlabel': 'Graduation Start Year',
            'ylabel': 'Average ENADE Grade (Normalized)'
        },

        {
            'file_name': processed_data_file,
            'plot_type': 'box',
            'x_col': 'CODIGO_CATEGORIA_ADMINISTRATIVA',
            'y_col': 'NOTA_ENEM_MATEMATICA',
            'output_file_name': 'box_enem_math_by_admin_category.png',
            'title': 'ENEM Math Score Distribution by Administrative Category',
            'xlabel': 'Administrative Category',
            'ylabel': 'ENEM Math Score'
        },

        {
            'file_name': processed_data_file,
            'plot_type': 'violin',
            'x_col': 'CODIGO_ORGANIZACAO_ACADEMICA',
            'y_col': 'NOTA_GERAL_ENADE',
            'output_file_name': 'violin_grade_by_academic_organization.png',
            'title': 'ENADE Grade Distribution by Academic Organization',
            'xlabel': 'Academic Organization',
            'ylabel': 'ENADE Grade (Normalized)'
        },

        {
            'file_name': processed_data_file,
            'plot_type': 'scatter',
            'x_col': 'NOTA_ENEM_MATEMATICA',
            'y_col': 'NOTA_GERAL_ENADE',
            'hue': 'CODIGO_CATEGORIA_ADMINISTRATIVA',
            'output_file_name': 'scatter_enade_vs_enem_math_by_category.png',
            'title': 'ENADE Grade vs. ENEM Math Grade by Administrative Category',
            'xlabel': 'ENEM Math Grade',
            'ylabel': 'ENADE Grade (Normalized)'
        }
    ]

    for config in graph_configs:
        try:
            generator.generate(config)
        except Exception as e:
            print(f"Failed to generate graph {config.get('output_file_name', 'N/A')}: {e}")


if __name__ == "__main__":
    main()
