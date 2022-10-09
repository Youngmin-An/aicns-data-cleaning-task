"""
Data Cleaning
# 1. Deduplication
# 2. Outlier Processing
# 3. Missing value Processing
"""

from func import *
import os

if __name__ == "__main__":
    # Initialize app
    spark = (
        SparkSession.builder.config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )

    app_conf = get_conf_from_evn()
    app_conf["sql_context"] = spark
    app_conf["APP_STAGE_ENUM"] = stage_map[app_conf["APP_STAGE_LEVEL"]]

    # Get feature metadata
    sensor = get_feature_metadata(app_conf)
    data_col_name = (
        "input_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"

    # Load data
    ts = load_raw_data(app_conf, sensor, time_col_name, data_col_name)

    # Deduplication
    print("dedup start")  # todo: use logging framework
    if app_conf["APP_STAGE_ENUM"].value >= Stage.DUP.value:
        dedup_df = deduplicate(ts, time_col_name, data_col_name, app_conf)
        save_to_data_warehouse(
            dedup_df, Stage.DUP, app_conf, time_col_name, data_col_name
        )
        if app_conf["APP_STAGE_ENUM"].value == Stage.DUP.value:
            save_to_data_warehouse(
                dedup_df, Stage.CLEANED, app_conf, time_col_name, data_col_name
            )
    print("dedup end")

    # Outlier processing
    print("deoutlier start")
    if app_conf["APP_STAGE_ENUM"].value >= Stage.OUTLIER.value:
        deout_df = process_outliers(dedup_df, time_col_name, data_col_name)
        save_to_data_warehouse(
            deout_df, Stage.OUTLIER, app_conf, time_col_name, data_col_name
        )
        if app_conf["APP_STAGE_ENUM"].value == Stage.OUTLIER.value:
            save_to_data_warehouse(
                deout_df, Stage.CLEANED, app_conf, time_col_name, data_col_name
            )
    print("deoutlier end")

    # Missing value processing
    print("demissing start")
    if app_conf["APP_STAGE_ENUM"].value >= Stage.MISSING.value:
        demiss_df = process_missing_values(deout_df, time_col_name, data_col_name)
        save_to_data_warehouse(
            demiss_df, Stage.MISSING, app_conf, time_col_name, data_col_name
        )
        if app_conf["APP_STAGE_ENUM"].value == Stage.MISSING.value:
            save_to_data_warehouse(
                demiss_df, Stage.CLEANED, app_conf, time_col_name, data_col_name
            )
    print("demissing end")

    # Finalize app
    spark.stop()
