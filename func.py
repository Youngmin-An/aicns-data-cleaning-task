from rawDataLoader import RawDataLoader, DatePartitionedRawDataLoader
from pyspark.sql import SparkSession, DataFrame
import os
import pendulum
from utils.feature_util import mongo_to_dict
from featureMetadataFetcher import FeatureMetadataFetcher, SensorMetadataFetcher
from feature.feature import Feature
from univariate.duplicate import DuplicateReport, DuplicateProcessor
from univariate.missing_value import MissingValueDetector, MissingValueReport, MissingValueHandler
from univariate.analyzer import AnalysisReport, RegularityAnalyzer, Analyzer
from enum import Enum
import pyspark.sql.functions as F
from helpers.url import HDFSBuilder

class Stage(Enum):
    CLEANED = -1
    DUP = 1
    OUTLIER = 2
    MISSING = 3


stage_map = {"DUP": Stage.DUP, "OUTLIER": Stage.OUTLIER, "MISSING": Stage.MISSING}


table_prefix_map = {
    Stage.CLEANED: "cleaned_",
    Stage.DUP: "cleaned_dup_",
    Stage.OUTLIER: "cleaned_outlier_",
    Stage.MISSING: "cleaned_missing_",
}  # todo: to env?


def get_feature_metadata(app_conf):
    fetcher = SensorMetadataFetcher()
    fetcher.get_or_create_conn(app_conf)
    sensors, positions = fetcher.fetch_metadata()
    sensor = next(
        filter(lambda sensor: sensor.ss_id == int(app_conf["FEATURE_ID"]), sensors)
    )  # todo: AICNS-33
    print("sensor: ", mongo_to_dict(sensor))
    return sensor


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Metadata
        conf["METADATA_HOST"] = os.getenv("METADATA_HOST")
        conf["METADATA_PORT"] = os.getenv("METADATA_PORT")
        conf["METADATA_TYPE"] = os.getenv("METADATA_TYPE", default="sensor")
        conf["METADATA_BACKEND"] = os.getenv("METADATA_BACKEND", default="MongoDB")
        # Data source
        conf["SOURCE_HOST"] = os.getenv("SOURCE_HOST")
        conf["SOURCE_PORT"] = os.getenv("SOURCE_PORT")
        conf["SOURCE_DATA_PATH_PREFIX"] = os.getenv(
            "SOURCE_DATA_PATH_PREFIX", default=""
        )
        conf["SOURCE_BACKEND"] = os.getenv("SOURCE_BACKEND", default="HDFS")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE")
        conf["start"] = pendulum.parse(start_datetime).in_timezone(conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime).in_timezone(conf["APP_TIMEZONE"])

        # todo: temp patch for day resolution parsing, so later with [AICNS-59] resolution will be subdivided.
        conf["end"] = conf["end"].subtract(minutes=1)

        # Etc App config
        conf["APP_STAGE_LEVEL"] = os.getenv(
            "APP_STAGE_LEVEL", default="MISSING"
        )  # DUP -> OUTLIER -> MISSING
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")

    except Exception as e:
        print(e)
        raise e
    return conf


def append_partition_cols(ts: DataFrame, time_col_name: str, data_col_name):
    return (
        ts.withColumn("datetime", F.from_unixtime(F.col(time_col_name) / 1000))
        .select(
            time_col_name,
            data_col_name,
            F.year("datetime").alias("year"),
            F.month("datetime").alias("month"),
            F.dayofmonth("datetime").alias("day"),
        )
        .sort(time_col_name)
    )


def save_to_data_warehouse(
    ts: DataFrame, stage: Stage, app_conf, time_col_name: str, data_col_name: str
):
    """
    Upsert data
    :param ts:
    :param stage:
    :param app_conf:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    # todo: transaction
    table_name = table_prefix_map[stage] + app_conf["FEATURE_ID"]
    SparkSession.getActiveSession().sql(f"CREATE TABLE IF NOT EXISTS {table_name} ({time_col_name} BIGINT, {data_col_name} DOUBLE) PARTITIONED BY (year int, month int, day int) STORED AS PARQUET LOCATION 'cleaning/{table_name}'")
    period = pendulum.period(app_conf["start"], app_conf["end"])

    # Create partition columns(year, month, day) from timestamp
    partition_df = append_partition_cols(ts, time_col_name, data_col_name)

    for date in period.range("days"):
        # Drop Partition for immutable task
        SparkSession.getActiveSession().sql(
            f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(year={date.year}, month={date.month}, day={date.day})"
        )
    # Save
    partition_df.write.format("hive").mode("append").insertInto(table_name)



def load_raw_data(app_conf, feature, time_col_name, data_col_name):
    loader = DatePartitionedRawDataLoader()
    loader.prepare_to_load(**app_conf)
    feature_raw_df = (
        loader.load_feature_data_by_object(
            start=app_conf["start"], end=app_conf["end"], feature=feature
        )
        .select(time_col_name, data_col_name)
        .sort(time_col_name)
    )
    return feature_raw_df


def deduplicate(
    ts: DataFrame, time_col_name: str, data_col_name: str, app_conf
):
    duplicate_report: DuplicateReport = DuplicateProcessor.detect_duplicates(
        ts, time_col_name, data_col_name
    )  # todo: send report to notification(?) system
    dropped_df = DuplicateProcessor.drop_duplicates(
        ts
    )  # todo: sorted return 'aicns-univariate-analyzer'
    return dropped_df


def process_outliers(ts: DataFrame, time_col_name: str, data_col_name: str) -> DataFrame:
    """

    :param ts:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    # todo: impl
    # todo: report
    return ts


def process_missing_values(ts: DataFrame, time_col_name: str, data_col_name: str) -> DataFrame:
    """

    :param ts:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    # todo: Dependency.. need precedence task that build regularity report (to report repository? metadata store?)
    regularity_report = __get_regularity_report(ts, time_col_name, data_col_name)
    # Detect missing values
    missing_value_detector = MissingValueDetector(regularity_report)
    miss_report = missing_value_detector.detect_missing_values(ts, time_col_name, data_col_name)  # todo: propagate miss report

    # Threat missing values
    missing_value_handler = MissingValueHandler()
    return missing_value_handler.handle_missing_value(miss_report.unmarked["marking_df"], time_col_name, data_col_name)


def __get_regularity_report(ts: DataFrame, time_col_name: str, data_col_name: str) -> AnalysisReport:
    """

    :param ts:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    # todo: Dependency.. need precedence task that build regularity report (to report repository? metadata store?)
    regularity_analyzer: Analyzer = RegularityAnalyzer()
    regularity_report: AnalysisReport = regularity_analyzer.analyze(
        ts=ts, time_col_name=time_col_name
    )
    return regularity_report
