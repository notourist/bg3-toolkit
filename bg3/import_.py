import os
import re
import xml.etree.ElementTree as ET  # noqa
from typing import Optional
import tempfile
import shutil
import xmltodict
import json

import click
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F  # noqa
from pyspark.sql.functions import array
from pyspark.sql.types import StructType, StructField, BooleanType, StringType

from bg3.main import main
from bg3.config import Config

cfg: Optional[Config] = None
spark: Optional[SparkSession] = None
sc: Optional[SparkContext] = None


@main.group(name="import")
@click.pass_context
def import_(ctx):
    global cfg
    global spark
    global sc
    cfg = ctx.obj["cfg"]
    spark = ctx.obj["spark"]
    sc = ctx.obj["sc"]


@import_.command()
@click.option(
    "-r",
    "--root-templates",
    is_flag=True,
)
@click.option("-c", "--classes", is_flag=True)
def lsx(root_templates: bool, classes: bool):
    table_names = []

    if root_templates:
        table_names.append("root_templates")
    if classes:
        table_names.append("classes")

    if not table_names:
        click.ClickException("No data to import.")

    def find_files(path: str, table_name: str):
        for root, dirs, files in os.walk(path):
            for file in files:
                if table_name == "root_templates" and "RootTemplates" in root:
                    yield os.path.join(root, file)
                if table_name == "classes" and "ClassDescriptions" in root:
                    yield os.path.join(root, file)

    def parse(table_name: str, files: list[str]):
        rdd = (
            sc.wholeTextFiles(",".join(files))
            .map(lambda x: x[1])
            .map(xmltodict.parse)
            .map(lambda x: json.dumps(x))
        )

        df = spark.read.json(rdd)

        df = (
            df.select(df.save.region.node.children.node.alias("node"))
            .select(F.explode("node").alias("node"))
            .select(
                F.expr("uuid()").alias("id"),
                F.col("node.attribute").alias("attribute"),
            )
            .select(F.col("id"), F.explode("attribute").alias("attribute"))
            .select(F.col("id"), F.col("attribute.*"))
            .withColumn("value", array("@value", "@handle"))
            .select(
                F.col("id"),
                F.col("@id"),
                F.explode("value").alias("value"),
            )
            .filter(F.col("value").isNotNull())
            .groupBy("id")
            .pivot("@id")
            .agg(F.first("value"))
            .drop("id")
        )

        df.write.jdbc(
            url=cfg.db.url,
            table=table_name,
            mode="overwrite",
        )

    for table_name in table_names:
        files = list(find_files(cfg.raw_data_path, table_name))
        if any(filter(lambda x: x.split("/")[-1].startswith("_"), files)):
            with tempfile.TemporaryDirectory() as tmpdir:
                new_files = []
                counter = 0
                for file in files:
                    shutil.copy(file, os.path.join(tmpdir, f"{counter}.xml"))
                    new_files.append(os.path.join(tmpdir, f"{counter}.xml"))
                    counter += 1
                parse(table_name, new_files)
        else:
            parse(table_name, files)


@import_.command()
def translations():
    def find_files():
        for root, dirs, files in os.walk(cfg.raw_data_path):
            for file in files:
                if ".loca.xml" in file:
                    yield os.path.join(root, file)

    def parse_file(content: str):
        root = ET.XML(content)

        for child in root:
            yield child.attrib.get("contentuid"), child.attrib.get(
                "version"
            ), child.text

    df = (
        sc.wholeTextFiles(",".join(find_files()))
        .map(lambda x: x[1])
        .flatMap(parse_file)
        .toDF(["contentuid", "version", "text"])
    )

    df.write.jdbc(
        url=cfg.db.url,
        table="translations",
        mode="overwrite",
    )


@import_.command()
@click.option("-w", "--weapons", is_flag=True)
@click.option("-a", "--armor", is_flag=True)
def items(weapons: bool, armor: bool):
    search_for = []
    if weapons:
        search_for.append("weapons")
    if armor:
        search_for.append("armor")
    if not search_for:
        raise click.ClickException("No data to import.")

    def find_files(search: str, honor_mode: bool = False):
        file_names = {
            "weapons": "Weapon.txt",
            "armor": "Armor.txt",
        }

        for root, dirs, files in os.walk(cfg.raw_data_path):
            for file in files:
                if file == file_names.get(search):
                    if (honor_mode and "Honour" in root) or (
                        not honor_mode and "Honour" not in root
                    ):
                        yield os.path.join(root, file)

    def parse_entry(entry: str) -> dict:
        type = re.search(r"type \"(.*)\"", entry).group(1)
        name = re.search(r"new entry \"(.*)\"", entry).group(1)

        using = re.search(r"using \"(.*)\"", entry)
        using = using.group(1) if using else None

        data = re.findall(r"data \"(.*?)\" \"(.*?)\"", entry)
        data = {d[0]: str(d[1]).strip() for d in data}

        return {
            "name": name,
            "type": type,
            "using": using,
            **data,
        }

    def resolve_using(obj, rows):
        if obj.get("using", None) is not None:
            merging_objs = []

            using_obj = next(
                filter(
                    lambda x: x["name"] == obj.get("using", None),
                    rows,
                )
            ).asDict()
            merging_objs.append(using_obj)

            while using_obj.get("using", None) is not None:
                using_obj = next(
                    filter(
                        lambda x: x["name"] == using_obj.get("using", None),
                        rows,
                    )
                ).asDict()
                merging_objs.append(using_obj)

            data = dict()
            for using_obj in reversed(merging_objs):
                if data is None:
                    data = using_obj.copy()
                else:
                    data.update({k: v for k, v in using_obj.items() if v is not None})

            data.update({k: v for k, v in obj.items() if v is not None})

            return data

        return obj

    for search in search_for:
        paths = list(find_files(search, honor_mode=False))

        raw_rdd = (
            sc.wholeTextFiles(",".join(paths))
            .map(lambda x: x[1])
            .map(lambda x: x.replace("\r\n", "\n"))
            .map(lambda l: l.split("\n\n"))
            .flatMap(lambda x: x)
            .filter(lambda x: x.startswith("new entry"))
            .map(parse_entry)
        )

        schema = StructType(
            [StructField("honour_mode", BooleanType())]
            + [
                StructField(k, StringType(), True)
                for k in raw_rdd.flatMap(lambda x: x.keys()).distinct().collect()
            ]
        )

        raw_df = raw_rdd.map(lambda x: {**x, "honour_mode": False}).toDF(schema=schema)

        raw_df.write.jdbc(
            url=cfg.db.url,
            table=f"raw_{search}",
            mode="overwrite",
        )

        rows = raw_df.collect()

        raw_df.rdd.map(lambda x: x.asDict()).map(lambda x: resolve_using(x, rows)).toDF(
            schema=schema
        ).write.jdbc(
            url=cfg.db.url,
            table=f"resolved_{search}",
            mode="overwrite",
        )

        paths = list(find_files(search, honor_mode=True))

        raw_df = (
            sc.wholeTextFiles(",".join(paths))
            .map(lambda x: x[1])
            .map(lambda x: x.replace("\r\n", "\n"))
            .map(lambda l: l.split("\n\n"))
            .flatMap(lambda x: x)
            .filter(lambda x: x.startswith("new entry"))
            .map(parse_entry)
            .map(lambda x: {**x, "honour_mode": True})
            .toDF(schema=schema)
        )

        raw_df.write.jdbc(
            url=cfg.db.url,
            table=f"raw_{search}",
            mode="append",
        )

        raw_df.rdd.map(lambda x: x.asDict()).map(lambda x: resolve_using(x, rows)).toDF(
            schema=schema
        ).write.jdbc(
            url=cfg.db.url,
            table=f"resolved_{search}",
            mode="append",
        )
