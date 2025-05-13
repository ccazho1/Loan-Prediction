import os
import subprocess
from src import config

def upload_and_copy_to_snowflake():
    abs_path = os.path.abspath(config.CSV_PATH)

    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"CSV file not found at: {abs_path}")

    sql_commands = f"""
        PUT file://{abs_path} @%{config.TABLE_NAME} AUTO_COMPRESS=TRUE;
        COPY INTO {config.TABLE_NAME}
        FROM @%{config.TABLE_NAME}
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
    """

    try:
        result = subprocess.run(
            [
                "snowsql",
                "-a", config.SNOWSQL_ACCOUNT,
                "-u", config.SNOWSQL_USER,
                "-q", sql_commands
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print("[INFO] Snowflake load successful:\n", result.stdout)

    except subprocess.CalledProcessError as e:
        print("[ERROR] Snowflake load failed:\n", e.stderr)
        raise
