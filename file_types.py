import os, io, re, csv, hashlib, uuid, pathlib, datetime as dt
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.tz import tzutc

UNIVERSAL_METADATA = {
    "software": "mat",
    "file_type": pa.string(),
    "teststand": pa.string(),
    "serial_number": pa.string(),
    "source_mtime_utc": pa.timestamp("us", tz="UTC"),
    "ingested_at_utc": pa.timestamp("us", tz="UTC"),
    "schema_version": pa.string(),
    "file_path": pa.string()
}

raw_data = pa.schema({
    **UNIVERSAL_METADATA,
    "Test_ID": pa.string(),
    "Model_Number": pa.string(),
    "Range(T)": pa.int64(),
    "Device_ID": pa.int64(),
    "Channel_ID": pa.int64(),
    "Port_ID": pa.int64(),
    "Unit_Temperature": pa.int64(),
    "Pressure_Unit": pa.string(),
})

report = pa.schema({
    **UNIVERSAL_METADATA,
    "Dev_ID": pa.int64(),
    "Model_Number": pa.string(),
    "Result": pa.string(), 
})

return_to_zero = pa.schema({
    **UNIVERSAL_METADATA,
    "5_sec": pa.float64(),
    "30_sec": pa.float64(),
    "2_min": pa.float64(),
    "5-30": pa.float64(),
    "30-120": pa.float64(),
    "5-120": pa.float64(),
})

cal = "pa.schema"({
    **UNIVERSAL_METADATA,
    "Time": pa.int64(),
    "Standard": pa.float64(),
    "UUT_Digital": pa.float64(),
    "UUT_Analog": pa.float()
})
