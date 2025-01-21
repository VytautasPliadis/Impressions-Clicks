import glob
import os
import shutil
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.functions import regexp_extract, input_file_name, to_date, col
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def process_files(spark: SparkSession, file_type: str, file_list: list[str], user_agent: str) -> DataFrame:
    """
    Processes either impression or click files, filtering, transforming, and aggregating data.

    Args:
        spark (SparkSession): The active Spark session.
        file_type (str): The type of files to process, either "impressions" or "clicks".
        file_list (list[str]): List of files to process.
        user_agent (str): The user agent string to filter by.

    Returns:
        DataFrame: Aggregated DataFrame with date, hour, and counts.
    """
    if not file_list:
        logging.warning(f"No {file_type} files found to process.")
        return spark.createDataFrame([], schema="date DATE, hour STRING, count INT")

    logging.info(f"Processing {len(file_list)} {file_type} files.")

    # Define regex pattern to extract date (YYYYMMDD) and hour (HH) from filenames
    regex_pattern = r"_(\d{8})(\d{2})"

    # Read files and process
    df = (
        spark.read.parquet(*file_list)
        .filter(col("device_settings.user_agent") == user_agent)
        .withColumn("filename", input_file_name())
        .withColumn("date", to_date(regexp_extract(col("filename"), regex_pattern, 1), "yyyyMMdd"))
        .withColumn("hour", regexp_extract(col("filename"), regex_pattern, 2))
        .groupBy("date", "hour")
        .count()
        .withColumnRenamed("count", f"{file_type}_count")
    )

    logging.info(f"{file_type.capitalize()} DataFrame processed successfully.")
    df.show(truncate=False)
    return df


def save_to_csv_per_day(df: DataFrame, output_path: str) -> None:
    """
    Saves each day's data to separate CSV files.

    Args:
        df (DataFrame): The DataFrame to save.
        output_path (str): The directory where CSV files will be saved.
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect all unique dates in the DataFrame
    dates = df.select("date").distinct().collect()

    for row in dates:
        day = row["date"]  # day is already in YYYY-MM-DD format
        temp_dir = output_dir / f"temp_{day}"
        final_file = output_dir / f"{day}.csv"

        # Filter data for this day and save to temporary directory
        df.filter(df.date == day) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(str(temp_dir))

        # Find the CSV file in the temporary directory (excluding metadata files)
        csv_files = list(Path(temp_dir).glob("*.csv"))
        if csv_files:
            # Move the CSV file to the final location
            shutil.move(str(csv_files[0]), str(final_file))

        # Clean up temporary directory
        shutil.rmtree(temp_dir)


def delete_processed_files(file_list: list[str]) -> None:
    """
    Deletes the processed parquet files from the raw_data folder.

    Args:
        file_list (list[str]): List of files to delete.
    """
    for file in file_list:
        try:
            os.remove(file)
            logging.info(f"Deleted file: {file}")
        except Exception as e:
            logging.error(f"Error deleting file {file}: {e}")
            raise


def main() -> None:
    """
    Main function to process impression and click files, join results, and save as CSV.
    """
    parser = argparse.ArgumentParser(description="Process impression and click data for specific user agent.")
    parser.add_argument("--user-agent", required=True, help="User agent string to filter by")
    args = parser.parse_args()

    raw_data_dir = Path("raw_data")
    output_dir = Path("output")
    temp_output = Path("temp")

    spark = (
        SparkSession.builder
        .appName("ImpressionClickProcessor")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    try:
        if not raw_data_dir.exists() or not any(raw_data_dir.iterdir()):
            raise ValueError("The 'raw_data' folder is empty or does not exist.")

        # List parquet files
        impression_files = glob.glob("raw_data/impressions*.parquet")
        click_files = glob.glob("raw_data/clicks*.parquet")

        # Process files with user agent
        impressions_df = process_files(spark, "impression", impression_files, args.user_agent)
        clicks_df = process_files(spark, "click", click_files, args.user_agent)

        # Join DataFrames
        final_df = (
            impressions_df.join(clicks_df, ["date", "hour"], "outer")
            .fillna({"impression_count": 0, "click_count": 0})
        )

        # Save CSV files
        save_to_csv_per_day(final_df, "output")

        logging.info("Final DataFrame:")
        final_df.show(truncate=False)

        # Delete processed parquet files
        delete_processed_files(impression_files + click_files)

    finally:
        if temp_output.exists():
            try:
                shutil.rmtree(temp_output)
                logging.info("Cleaned up temporary directory")
            except Exception as e:
                logging.warning(f"Warning: Could not clean up temporary directory: {e}")
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
