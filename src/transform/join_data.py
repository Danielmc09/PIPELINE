import pandas as pd
from src.utils.logger import logger


class DataJoiner:
    def __init__(self, user_id_col="user_id", value_prop_col="value_prop"):
        self.user_id_col = user_id_col
        self.value_prop_col = value_prop_col

    def join_data(self, prints_df, taps_df, payments_df):
        """
        Realiza el join entre los DataFrames de prints, taps y payments para consolidar la información
        en un solo DataFrame maestro.

        Args:
            prints_df (pd.DataFrame): DataFrame de visualizaciones (prints).
            taps_df (pd.DataFrame): DataFrame de clics (taps).
            payments_df (pd.DataFrame): DataFrame de pagos (payments).

        Returns:
            pd.DataFrame: DataFrame maestro con la información consolidada de prints, taps y payments.
        """
        logger.info("Iniciando el join entre prints, taps y payments...")

        # Verificar que las columnas necesarias estén en los DataFrames
        required_columns = [self.user_id_col, self.value_prop_col]

        for col in required_columns:
            if col not in prints_df.columns:
                logger.error(f"Columna '{col}' no encontrada en prints_df.")
                raise KeyError(f"Columna '{col}' no encontrada en prints_df.")
            if col not in taps_df.columns:
                logger.error(f"Columna '{col}' no encontrada en taps_df.")
                raise KeyError(f"Columna '{col}' no encontrada en taps_df.")

        if 'pay_date' not in payments_df.columns:
            logger.error("Columna 'pay_date' no encontrada en payments_df.")
            raise KeyError("Columna 'pay_date' no encontrada en payments_df.")

        # Join entre prints y taps usando user_id y value_prop
        prints_taps_merged = pd.merge(
            prints_df,
            taps_df,
            on=[self.user_id_col, self.value_prop_col],
            how="left",
            suffixes=("_prints", "_taps")
        )

        logger.info(f"Join entre prints y taps completado. Columnas actuales: {prints_taps_merged.columns.tolist()}")

        # Join entre prints_taps_merged y payments usando user_id y value_prop
        final_merged_df = pd.merge(
            prints_taps_merged,
            payments_df,
            left_on=[self.user_id_col, self.value_prop_col],
            right_on=[self.user_id_col, self.value_prop_col],
            how="left"
        )

        # Eliminamos la columna duplicada pay_date de payments
        final_merged_df = final_merged_df.drop(columns=["pay_date"])

        logger.info("Join final completado. Estructura del DataFrame maestro:")
        logger.info(final_merged_df.info())
        logger.info("Primeras filas del DataFrame maestro:")
        logger.info(final_merged_df.head())

        return final_merged_df
