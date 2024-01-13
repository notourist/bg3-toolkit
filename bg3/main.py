import os
from typing import Optional

import click
from pyspark import SparkContext
from pyspark.sql import SparkSession

from bg3.config import Config

DEFAULT_CONFIG_PATH = "config.yaml"
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
cfg: Optional[Config] = None
spark: Optional[SparkSession] = None
sc: Optional[SparkContext] = None


@click.group()
@click.option("-c", "--config", type=click.Path(), default=DEFAULT_CONFIG_PATH)
@click.pass_context
def main(ctx, config: str):
    global cfg
    global spark
    global sc
    cfg = Config.load(config)
    spark = (
        SparkSession.builder.appName("BG3")
        .config("spark.jars", ", ".join(cfg.spark.jars))
        .config("spark.jars.packages", ",".join(cfg.spark.packages))
        .config("spark.driver.extraClassPath", cfg.spark.jdbc_driver_path)
        .config("spark.driver.memory", cfg.spark.memory)
        .getOrCreate()
    )
    sc = spark.sparkContext
    ctx.obj = {
        "cfg": cfg,
        "spark": spark,
        "sc": sc,
    }
