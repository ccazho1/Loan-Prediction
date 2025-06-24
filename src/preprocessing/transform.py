import pandas as pd
from src.data_io.fetch import fetch_raw_data
from src.data_io.save import save_to_csv, save_to_snowflake
import src.features
from src.feature_builder import fb



def transformation(df: pd.DataFrame) -> pd.DataFrame:
    # remove symbols to convert to float (monthly debt column)
    df["MONTHLY_DEBT"] = (
        df["MONTHLY_DEBT"]
        .str.replace("[$,]", "", regex=True)
        .astype(float)
    )
    df["MONTHLY_DEBT"] = df["MONTHLY_DEBT"].astype(float)
    df["MONTHLY_DEBT"] = df["MONTHLY_DEBT"].fillna(df["MONTHLY_DEBT"].median())

    # deal with overscaled credit scores
    df["CREDIT_SCORE"] = df["CREDIT_SCORE"].apply(lambda x: x / 10 if x > 850 else x)

    df["CREDIT_SCORE_MISSING"] = df["CREDIT_SCORE"].isnull().astype(int)


    # deal with null annual income
    df["ANNUAL_INCOME_MISSING"] = df["ANNUAL_INCOME"].isnull().astype(int)


    # cases when both annual income and credit score are missing (21338 records)
    df["MISSING_INCOME_AND_CREDIT_SCORE"] = (
        df["CREDIT_SCORE"].isnull() & df["ANNUAL_INCOME"].isnull()
    ).astype(int)
    
    # fill missing values with median
    median_credit = df["CREDIT_SCORE"].median()
    median_income = df["ANNUAL_INCOME"].median()
    df["ANNUAL_INCOME"].fillna(median_income, inplace=True)
    df["CREDIT_SCORE"].fillna(median_credit, inplace=True)

    # deal with years_in_job
    df["YEARS_IN_JOB"] = pd.to_numeric(df["YEARS_IN_JOB"], errors="coerce")
 
    # deal with loan_status
    loan_status_map = {
        "Fully Paid": 0,
        "Charged Off": 1
    }
    df["LOAN_STATUS"] = df["LOAN_STATUS"].map(loan_status_map)

    # deal with current_loan_amount
    # Flag them
    df["LOAN_AMOUNT_PLACEHOLDER"] = (df["CURRENT_LOAN_AMOUNT"] == 99999999).astype(int)
    # Impute
    median_val = df[df["CURRENT_LOAN_AMOUNT"] != 99999999]["CURRENT_LOAN_AMOUNT"].median()
    df["CURRENT_LOAN_AMOUNT"] = df["CURRENT_LOAN_AMOUNT"].replace(99999999, median_val)

    return df


if __name__ == "__main__":

    df_raw = fetch_raw_data()
    df_clean = transformation(df_raw)
    df_feat = fb.run(df_clean)
    save_to_csv(df_feat)
    save_to_snowflake(df_feat)
