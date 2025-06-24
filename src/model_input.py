import pandas as pd
import numpy as np

def check_nan(X):
    print("[CHECK] Any NaN?", np.isnan(X).any())
    print("[CHECK] Any +inf?", np.isinf(X).any())
    print("[CHECK] Any -inf?", np.isinf(X).any())
    print("[CHECK] Columns with inf or NaN:")
    print(X.columns[np.isinf(X).any(axis=0) | np.isnan(X).any(axis=0)])

def select_model_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Drops unnecessary or misleading columns.
    """

    columns_to_drop = [
        'LOAN_ID',
        'CUSTOMER_ID',
        'LOAN_STATUS',  # label
        'LOAN_AMOUNT_PLACEHOLDER',
        'MISSING_INCOME_AND_CREDIT_SCORE',
        'CREDIT_SCORE_MISSING',
        'ANNUAL_INCOME_MISSING',
        'ANNUAL_INCOME',
        'MONTHS_SINCE_LAST_DELINQUENT'
    ]

    drop_cols = [col for col in columns_to_drop if col in df.columns]

    y = df["LOAN_STATUS"]

    X = df.drop(columns=drop_cols)

    return X, y
