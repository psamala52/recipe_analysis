from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import isodate

def iso_to_minutes(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        return int(duration.total_seconds() // 60)
    except:
        return None

iso_to_minutes_udf = udf(iso_to_minutes, IntegerType())

def duplicate_null_drop(df):
    df = df.dropDuplicates().na.drop()
    return df

