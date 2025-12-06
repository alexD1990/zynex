from DCheck.core.engine import run_engine

def validate_spark(df, table_name=None):
    """
    Public Spark API for validation.
    """
    return run_engine(df, table_name=table_name)
