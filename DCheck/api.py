from DCheck.core.engine import run_engine

def validate_spark(df):
    """
    Public Spark API for validation.
    """
    return run_engine(df)
