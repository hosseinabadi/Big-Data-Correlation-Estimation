import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import tarfile
from io import BytesIO
from tqdm import tqdm

schemas = {
    "bbo": {
        "xltime": pl.Float64,
        "bid-price": pl.Float64,
        "bid-volume": pl.Int32,
        "ask-price": pl.Float64,
        "ask-volume": pl.Int32,
    },
    "trade": {
        "xltime": pl.Float64,
        "trade-price": pl.Float64,
        "trade-volume": pl.Int32,
        "trade-stringflag": pl.Utf8,
        "trade-rawflag": pl.Utf8,
    },
}

def set_timeseries(data):

    base_date = datetime(1899, 12, 30)

    data = data.with_columns(
        (base_date + pl.col("xltime").cast(float) * timedelta(days=1)).alias("datetime")
    )

    data = data.with_columns(pl.col("datetime").dt.convert_time_zone("America/New_York"))
    
    data = data.drop("xltime").sort("datetime")

    return data


def validate_and_fix_schema(df, expected_schema):
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
        
        
        if df[col].dtype != expected_type:
            if expected_type == pl.Float64:
                df = df.with_columns(
                    pl.when(pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(["", "()", None]))
                    .then(None)
                    .otherwise(pl.col(col).cast(pl.Utf8))
                    .str.replace_all(r"[^\d.]", "")  # Remove non-numeric characters
                    .cast(pl.Float64)  # Cast back to Float64
                    .alias(col)
                )
            # Handle Int32 columns
            elif expected_type == pl.Int32:
                df = df.with_columns(
                    pl.when(pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(["", "()", None]))
                    .then(None)
                    .otherwise(pl.col(col).cast(pl.Utf8))
                    .str.replace_all(r"[^\d]", "")  # Remove non-numeric characters
                    .cast(pl.Int32)  # Cast back to Int32
                    .alias(col)
                )
            else:
                # Cast other types directly
                df = df.with_columns(df[col].cast(expected_type).alias(col))
    return df

def get_buckets(assets_data, deltat = 5, only_trading_hours = True, opening_hour = "10:00:00", closing_hour = "15:30:00"):
    clean_assets = {}
    for asset_name, df in assets_data.items():
        date_list = df["datetime"].dt.date().unique()

        df = df.with_columns(
            (df["datetime"].cast(float)/1e6 // deltat * deltat).alias('time-bucket')
        )

        result = df.group_by("time-bucket").agg([
            ((((pl.col("ask-price")*pl.col("ask-volume")).sum() / pl.col("ask-volume").sum()) + 
            ((pl.col("bid-price")*pl.col("bid-volume")).sum() / pl.col("bid-volume").sum())) / 2).alias("weighted-avg-price")
        ])
        
        

        result = result.with_columns(
            (result["time-bucket"].map_elements(lambda x: datetime.utcfromtimestamp(x))),
            result["weighted-avg-price"].cast(float).alias("weighted-avg-price")
        )
        result = result.with_columns(pl.col("time-bucket").dt.convert_time_zone("America/New_York"))
        result = result.fill_nan(None)
        result = result.sort("time-bucket")

        # Convert start and end dates to datetime
        start_time = datetime.strptime("2007-01-01", "%Y-%m-%d")
        end_time = datetime.strptime("2012-12-31", "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        # Generate time series with pl.date_range
        time_series = pl.DataFrame({
            "time-bucket": pl.datetime_range(
                start=start_time,
                end=end_time,
                interval=f"{deltat}s",  # Step size in seconds
                eager=True,
                time_zone = "America/New_York"
            )
        })
        
        result = result.join(time_series, on="time-bucket", how="full")
        result = result.select([
            pl.col("weighted-avg-price").alias("weighted-avg-price"),  
            pl.col("time-bucket_right").alias("time-bucket") 
        ])
        result = result.sort("time-bucket")
        result = result.select(pl.all().forward_fill())
        result = result.drop_nulls()

        if only_trading_hours:
            hh_open,mm_open,ss_open = [float(x) for x in opening_hour.split(":")]
            hh_close,mm_close,ss_close = [float(x) for x in closing_hour.split(":")]

            seconds_open=hh_open*3600+mm_open*60+ss_open
            seconds_close=hh_close*3600+mm_close*60+ss_close

            result = result.filter(pl.col('time-bucket').dt.hour().cast(float)*3600+pl.col('time-bucket').dt.minute().cast(float)*60+pl.col('time-bucket').dt.second()>=seconds_open,
                        pl.col('time-bucket').dt.hour().cast(float)*3600+pl.col('time-bucket').dt.minute().cast(float)*60+pl.col('time-bucket').dt.second()<=seconds_close)


        result = result.filter(
            pl.col("time-bucket").dt.date().is_in(date_list)
        )
        clean_assets[asset_name] = result.sort("time-bucket")
    return clean_assets

def remove_outliers(bucketed_data):
    clean_data = {}
    for ticker, df in bucketed_data.items():
        upper_bounds = {"EDEN": None, "EFNL": 900, "EIS": 100, "EUSA": {2010: 27.8, 2011:35}, "EWA": 900, "EWC": {2009:28, 2010: 900, 2011: 900, 2012: 900}, "EWD": 900, "EWG": 900, "EWH": 25, "EWI": {2011: 21.5}, "EWJ": 900, "EWK": 22.5, "EWL": 75, "EWN": 75, "EWO": {2009: 50, 2010: 50, 2011: 25, 2012: 21.5}, "EWP": 55, "EWQ": 29, "EWS": 20, "EWT": {2009: 14}, "EWU": {2009: 100, 2012: 20}, "EWW": 900, "EWY":900, "EWZ": 900, "INDA": {2012: 30}, "MCHI": 900}
        lower_bounds = {"EDEN": None, "EFNL": -900, "EIS": -900, "EUSA": -900, "EWA": -900, "EWC": -900, "EWD": {2010: 13}, "EWG": {2009:13.2}, "EWH": -900, "EWI": -900, "EWJ": -900, "EWK": {2010: 9}, "EWL": {2011: 15}, "EWN": {2010: 15, 2011: 14}, "EWO": -900, "EWP": -900, "EWQ": -900, "EWS": {2011: 8}, "EWT": {2011: 9}, "EWU": -900, "EWW": -900, "EWY":-900, "EWZ": -900, "INDA": -900, "MCHI": -900} 
        # Define thresholds for each year
        upper_bound = upper_bounds.get(ticker, 900)
        lower_bound = lower_bounds.get(ticker, -900)

        if upper_bound == None:
            print("This data is problematic")
            upper_bound = 900
            lower_bound = -900
        if type(upper_bound) == int or type(upper_bound) == float:
            upper_bound = {year: upper_bound for year in range(2009, 2013)}
        if type(lower_bound) == int or type(lower_bound) == float:
            lower_bound = {year: lower_bound for year in range(2009, 2013)}
        

        # Extract year from the time-bucket column
        df = df.with_columns(
            pl.col("time-bucket").dt.year().alias("year")
        )
        
        # Apply different thresholds for each year
        result = df.with_columns(
            pl.when((pl.col("weighted-avg-price") > pl.col("year").map_elements(lambda year: upper_bound.get(year, float('inf')))) | (pl.col("weighted-avg-price") < pl.col("year").map_elements(lambda year: lower_bound.get(year, float('-inf')))))
            .then(None)
            .otherwise(pl.col("weighted-avg-price"))
            .alias("weighted-avg-price")
        ).drop("year")


        result = result.select(pl.all().forward_fill())
        clean_data[ticker] = result.sort("time-bucket")
    return clean_data


def get_raw_data(years, asset_names, print_log = True):

    assets_data = {}

    # List of yearly `.tar` files to process
    yearly_tar_files = [f"Data/ETFs/ETFs-{year}.tar" for year in years]
    # Step 1: Iterate through yearly `.tar` files
    for yearly_tar_path in yearly_tar_files:
        if print_log:
            print(f"Processing yearly tar: {yearly_tar_path}")
        
        with tarfile.open(yearly_tar_path, "r") as outer_tar:
            for asset in asset_names:
                # Step 2: Iterate through files in the yearly `.tar`
                for member in outer_tar.getmembers():
                    if member.isfile() and member.name.endswith(".tar") and asset in member.name:
                        if print_log:
                            print(f"Processing inner tar: {member.name}")
                        
                        if "bbo" in member.name:
                            expected_schema = schemas["bbo"]
                        else:
                            if print_log:
                                print(f"Skipping non-price file type: {member.name}")
                            continue
                        
                        # Step 3: Extract the inner `.tar` file
                        inner_tar_data = BytesIO(outer_tar.extractfile(member).read())
                        with tarfile.open(fileobj=inner_tar_data, mode="r") as inner_tar:
                            parquet_files = []
                            
                            for inner_member in inner_tar.getmembers():
                                # Look for `.parquet` files
                                if inner_member.isfile() and inner_member.name.endswith(".parquet"):
                                    parquet_data = BytesIO(inner_tar.extractfile(inner_member).read())
                                    df = pl.read_parquet(parquet_data)
                                    
                                    # Validate and fix schema
                                    try:
                                        df = validate_and_fix_schema(df, expected_schema)
                                        parquet_files.append(df)
                                    except ValueError as e:
                                        print(f"Schema error in file {inner_member.name}: {e}")
                                        continue
                            
                            # Step 4: Concatenate all Parquet files for this asset in the year
                            if parquet_files:
                                combined_df = pl.concat(parquet_files, how="vertical")

                                combined_df = set_timeseries(combined_df)
                                # Extract asset name (e.g., `EWW.P_bbo` from `EWW.P_bbo_2007.tar`)
                                asset_name = member.name.rsplit("_", 1)[0]
                                
                                # Append to the existing data for the same asset across years
                                if asset_name in assets_data:
                                    assets_data[asset_name] = pl.concat([assets_data[asset_name], combined_df], how="vertical")
                                else:
                                    assets_data[asset_name] = combined_df
                                if print_log:
                                    print(f"Combined DataFrame for {asset_name} now has {len(assets_data[asset_name])} rows.")

    assets_data = {key.split(".")[1][1:]:value for key,value in assets_data.items()}

    return assets_data







def get_average_diffs(assets_data):
    average_diffs = {}
    for asset_name, df in assets_data.items():
        df = df.with_columns(pl.col("datetime").diff().alias("time_diff"))

        # Calculate the average difference
        average_diff = df["time_diff"].drop_nulls().mean()

        print(f"Average time difference for {asset_name}: {average_diff}")
        average_diffs[asset_name] = average_diff
    return average_diffs
