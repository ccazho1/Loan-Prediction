import pandas as pd
import numpy as np
from src.preprocessing.feature_builder import fb

@fb.register
def log_annual_income(df):
    # ensure no neg values exisiting in dataset
    df["ANNUAL_INCOME"] = df["ANNUAL_INCOME"].apply(lambda x: max(x, 0))
    df["LOG_ANNUAL_INCOME"] = np.log1p(df["ANNUAL_INCOME"])
    
    return df

@fb.register
def group_purpose(df):
    purpose_map = {
        "debt consolidation": "debt_consolidation",
        "home improvements": "housing",
        "business loan": "business",
        "small_business": "business",
        "buy a car": "vehicle",
        "medical bills": "medical",
        "take a trip": "vacation",
        "vacation": "vacation",
        "major_purchase": "major_purchase",
        "educational expenses": "education",
        "buy house": "housing",
        "moving": "housing",
        "wedding": "wedding",
        "renewable_energy": "renewable_energy",
        "other": "other"
    }

    df["PURPOSE"] = df["PURPOSE"].str.strip().str.lower() # standardize casing and strip whitespace first
    df["PURPOSE"] = df["PURPOSE"].map(purpose_map).fillna("other") # groupinng purpose categories
    
    return df

@fb.register
def encode_home_ownership(df):
    home_ownership_map = {
        'HaveMortgage': 'MORTGAGE',
        'Rent': 'RENT',
        'Home Mortgage': 'MORTGAGE',
        'Own Home': 'OWN'
    }
    df["HOME_OWNERSHIP"] = df["HOME_OWNERSHIP"].str.strip()
    df["HOME_OWNERSHIP"] = df["HOME_OWNERSHIP"].map(home_ownership_map).fillna("other")

    return df

@fb.register
def encode_term(df):
    term_map = {
        "short term": "SHORT",
        "long term": "LONG"
    }
    df["TERM"] = df["TERM"].str.strip().str.lower()
    df["TERM"] = df["TERM"].map(term_map)

    return df

@fb.register
def one_hot_encode_categoricals(df):
    # one hot encoding columns for modeling: Home ownership, Term, Purpose
    df = pd.get_dummies(df, columns=["HOME_OWNERSHIP", "TERM", "PURPOSE"], drop_first=True)
    
    return df

@fb.register
def debt_to_income(df):
    # Debt-to-Income Ratio	monthly_debt / (annual_income / 12)
    df["DEBT_TO_INCOME"] = df["MONTHLY_DEBT"] / (df["ANNUAL_INCOME"] / 12)
    return df

@fb.register
def credit_utilization(df):
    # Credit Utilization	current_credit_balance / maximum_open_credit
    # .replace(0, np.nan) added on in the cases where maximum_open_credit is 0
    df["CREDIT_UTILIZATION"] = df["CURRENT_CREDIT_BALANCE"] / df["MAXIMUM_OPEN_CREDIT"].replace(0, np.nan)

    # ensure CREDIT_UTILIZATION contains only valid, finite numbers or NaNs
    df["CREDIT_UTILIZATION"].replace([np.inf, -np.inf], np.nan, inplace=True)
    return df

@fb.register
def delinquency_recency_flag(df):
    # months_since_last_delinquent < 12 → binary	Flags recent bad behavior
    df["RECENT_DELINQUENCY_FLAG"] = df["MONTHS_SINCE_LAST_DELINQUENT"] < 12
    return df

@fb.register
def credit_problem_score(df):
    df["CREDIT_PROBLEM_SCORE"] = (
        df["NUMBER__CREDIT_PROBLEMS"] * 1 +
        df["BANKRUPTCIES"] * 3 +
        df["TAX_LIENS"] * 2
    )
    return df

@fb.register
def job_stability(df):
    # how much should this weigh?
    # years_in_job >= 5 → binary	Captures employment consistency
    df["JOB_STABILITY"] = df["YEARS_IN_JOB"] >= 5
    return df

@fb.register
def high_credit_risk(df):
    # credit_score < 600 → binary	Adds business logic risk tag
    df["CREDIT_RISK"] = df["CREDIT_SCORE"] < 600
    return df

@fb.register
def impute_nan(df):
    na_cols = ["YEARS_IN_JOB", "MAXIMUM_OPEN_CREDIT", "TAX_LIENS", "CREDIT_UTILIZATION", "CREDIT_PROBLEM_SCORE"]

    for col in na_cols:
        median = df[col].median()
        df[col].fillna(median, inplace=True)

    return df
# def engineer_features(df):
    # df = fb.run(df)
    
