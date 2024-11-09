import dask.dataframe as dd
from datetime import timedelta
import pandas as pd
from src.utils.logger import logger

class MetricsAggregator:
    def __init__(self, user_id_col="user_id", value_prop_col="value_prop"):
        self.user_id_col = user_id_col
        self.value_prop_col = value_prop_col
        logger.info("Inicializando MetricsAggregator con columnas: user_id_col=%s, value_prop_col=%s",
                    user_id_col, value_prop_col)

    def validate_columns(self, df, required_columns):
        """
        Valida que los DataFrames contengan las columnas necesarias.
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise KeyError(f"Faltan las columnas requeridas: {missing_columns}")
        logger.info("Validación de columnas exitosa: Todas las columnas requeridas están presentes.")

    def calculate_click_indicator(self, dataframe, prints_last_week):
        """
        Calcula el indicador de clics (clicked) para cada print en la última semana utilizando Dask.
        """
        self.validate_columns(dataframe, ['day_taps', 'user_id', 'value_prop'])
        self.validate_columns(prints_last_week, ['day_prints', 'user_id', 'value_prop'])

        # Convertir a Dask DataFrames
        ddf = dd.from_pandas(dataframe, npartitions=4)
        prints_last_week_ddf = dd.from_pandas(prints_last_week, npartitions=4)

        # Marcar los registros con clics y hacer el merge con prints_last_week
        ddf['clicked'] = (~ddf['day_taps'].isnull()).map({True: "Sí", False: "No"})  # Usar `isnull()` y negarlo
        clicks_merged = dd.merge(prints_last_week_ddf,
                                 ddf[['user_id', 'value_prop', 'day_prints', 'clicked']],
                                 on=['user_id', 'value_prop', 'day_prints'],
                                 how='left')
        clicks_merged['clicked'] = clicks_merged['clicked'].fillna("No")
        return clicks_merged[['user_id', 'value_prop', 'day_prints', 'clicked']].compute()

    def calculate_view_counts(self, dataframe, prints_last_week, weeks=3):
        """
        Calcula la cantidad de vistas para los registros de las últimas tres semanas utilizando Dask.
        """
        self.validate_columns(dataframe, ['day_prints', self.user_id_col, self.value_prop_col])
        self.validate_columns(prints_last_week, ['day_prints', self.user_id_col, self.value_prop_col])

        # Convertir a Dask DataFrames
        ddf = dd.from_pandas(dataframe, npartitions=4)
        prints_last_week_ddf = dd.from_pandas(prints_last_week, npartitions=4)

        # Filtrar registros de las últimas tres semanas y realizar el merge
        last_date = ddf['day_prints'].max().compute()
        start_date = last_date - pd.Timedelta(weeks=weeks)
        ddf_recent = ddf[(ddf['day_prints'] >= start_date) & (ddf['day_prints'] <= last_date)]

        merged_df = dd.merge(ddf_recent,
                             prints_last_week_ddf[['user_id', 'value_prop', 'day_prints']],
                             on=['user_id', 'value_prop'],
                             suffixes=('', '_print_last_week'))
        merged_df = merged_df[merged_df['day_prints'] < merged_df['day_prints_print_last_week']]

        # Contar vistas y usar rename para ajustar el nombre
        view_counts = (merged_df.groupby(['user_id', 'value_prop', 'day_prints_print_last_week'])
                       .size()
                       .reset_index()
                       .rename(columns={0: 'view_count', 'day_prints_print_last_week': 'day_prints'}))
        return view_counts[['user_id', 'value_prop', 'day_prints', 'view_count']].compute()

    def calculate_click_counts_per_value_prop(self, dataframe, prints_last_week, weeks=3):
        """
        Calcula la cantidad de clics por usuario y value_prop en las últimas tres semanas utilizando Dask.
        """
        self.validate_columns(dataframe, ['day_taps', self.user_id_col, self.value_prop_col])
        self.validate_columns(prints_last_week, ['day_prints', self.user_id_col, self.value_prop_col])

        # Convertir a Dask DataFrames
        ddf = dd.from_pandas(dataframe, npartitions=4)
        prints_last_week_ddf = dd.from_pandas(prints_last_week, npartitions=4)

        # Filtrar registros de las últimas tres semanas y realizar el merge
        last_date = ddf['day_prints'].max().compute()
        start_date = last_date - pd.Timedelta(weeks=weeks)
        ddf_recent = ddf[(ddf['day_taps'] >= start_date) & (ddf['day_taps'] <= last_date)]

        merged_df = dd.merge(ddf_recent,
                             prints_last_week_ddf[['user_id', 'value_prop', 'day_prints']],
                             on=['user_id', 'value_prop'],
                             suffixes=('', '_print_last_week'))
        merged_df = merged_df[merged_df['day_taps'] < merged_df['day_prints_print_last_week']]

        # Contar clics y usar rename para ajustar el nombre
        click_counts = (merged_df.groupby(['user_id', 'value_prop', 'day_prints_print_last_week'])
                        .size()
                        .reset_index()
                        .rename(columns={0: 'click_count', 'day_prints_print_last_week': 'day_prints'}))
        return click_counts[['user_id', 'value_prop', 'day_prints', 'click_count']].compute()

    def calculate_cumulative_payment_amounts(self, dataframe, pays_df, weeks=3):
        """
        Calcula el importe acumulado por usuario y value_prop en las últimas tres semanas utilizando Dask.
        """
        self.validate_columns(pays_df, ['pay_date', 'total', 'user_id', 'value_prop'])
        self.validate_columns(dataframe, ['day_prints', 'user_id', 'value_prop'])

        # Convertir a Dask DataFrames
        ddf = dd.from_pandas(dataframe, npartitions=4)
        pays_ddf = dd.from_pandas(pays_df, npartitions=4)

        # Crear columna de referencia temporal y realizar el merge
        ddf['three_weeks_ago'] = ddf['day_prints'] - pd.Timedelta(weeks=weeks)
        merged_df = dd.merge(pays_ddf,
                             ddf[['user_id', 'value_prop', 'day_prints', 'three_weeks_ago']],
                             left_on=['user_id', 'value_prop'],
                             right_on=['user_id', 'value_prop'],
                             how='inner',
                             suffixes=('', '_print'))
        filtered_pays = merged_df[
            (merged_df['pay_date'] >= merged_df['three_weeks_ago']) & (merged_df['pay_date'] < merged_df['day_prints'])
            ]

        # Sumar pagos y renombrar la columna después de reset_index
        cumulative_amounts = (filtered_pays.groupby(['user_id', 'value_prop', 'day_prints'])
                              ['total'].sum().reset_index()
                              .rename(columns={'total': 'cumulative_payment_amount'}))
        return cumulative_amounts[['user_id', 'value_prop', 'day_prints', 'cumulative_payment_amount']].compute()


