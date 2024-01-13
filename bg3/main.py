import os
import re
import xml.etree.ElementTree as ET  # noqa
from typing import Optional

import click
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F  # noqa
from pyspark.sql.functions import array
from pyspark.sql.types import StructType, StructField, BooleanType, StringType

from bg3.config import Config

DEFAULT_CONFIG_PATH = "config.yaml"
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
cfg: Optional[Config] = None
spark: Optional[SparkSession] = None
sc: Optional[SparkContext] = None


@click.group()
@click.option("-c", "--config", type=click.Path(), default=DEFAULT_CONFIG_PATH)
def main(config: str):
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


@main.group()
def search():
    pass


@search.command()
def shell():
    import readline  # noqa

    translations = spark.read.jdbc(
        url=cfg.db.url,
        table="translations",
    )

    root_templates = spark.read.jdbc(
        url=cfg.db.url,
        table="root_templates",
    )

    weapons = spark.read.jdbc(
        url=cfg.db.url,
        table="resolved_weapons",
    )

    armor = spark.read.jdbc(
        url=cfg.db.url,
        table="resolved_armor",
    )

    translations.registerTempTable("translations")
    root_templates.registerTempTable("root_templates")
    weapons.registerTempTable("resolved_weapons")
    armor.registerTempTable("resolved_armor")

    armor.join(
        root_templates.withColumnRenamed("Name", "templ_name"),
        F.col("Stats") == armor.name,
    ).join(
        translations, translations.contentuid == root_templates.DisplayName, "left"
    ).registerTempTable(
        "armor"
    )

    weapons.join(
        root_templates.withColumnRenamed("Name", "templ_name"),
        F.col("Stats") == weapons.name,
    ).join(
        translations, translations.contentuid == root_templates.DisplayName, "left"
    ).registerTempTable(
        "weapons"
    )

    def completer(text, state):
        options = (
            [
                t[0]
                for t in spark.sql("show tables").select("tableName").collect()
                if t[0].startswith(text)
            ]
            + [c for c in armor.columns if c.startswith(text)]
            + [c for c in weapons.columns if c.startswith(text)]
            + [c for c in translations.columns if c.startswith(text)]
            + [c for c in root_templates.columns if c.startswith(text)]
        )
        try:
            return f'"{options[state]}" ' if " " in options[state] else options[state]
        except IndexError:
            return None

    readline.parse_and_bind("tab: menu-complete")
    readline.set_completer(completer)

    quit = False
    while not quit:
        inpt = input(">>> ").strip()
        if inpt == "exit":
            quit = True
            continue

        show_count_match = re.search(r"/(?:s|show) (\d+|all)", inpt)
        show_count = (
            (
                -1
                if show_count_match.group(1) == "all"
                else int(show_count_match.group(1))
            )
            if show_count_match
            else 10
        )
        inpt = re.sub(r"/(s|show) (\d+|all)", "", inpt)

        vertical_match = re.search(r"(/vertical)|(/v)", inpt)
        vertical = vertical_match is not None

        inpt = re.sub(r"(/vertical)|(/v)", "", inpt)

        truncate_match = re.search(r"(/truncate)|(/t)", inpt)
        truncate = truncate_match is not None
        inpt = re.sub(r"(/truncate)|(/t)", "", inpt)

        drop_null_match = re.search(r"(/dropnull)|(/d)", inpt)
        drop_null = drop_null_match is not None
        inpt = re.sub(r"(/dropnull)|(/d)", "", inpt)

        try:
            df = spark.sql(inpt)
            if drop_null:
                df = spark.createDataFrame(df.toPandas().dropna(axis=1))
            df.show(
                df.count() if show_count == -1 else show_count,
                vertical=vertical,
                truncate=truncate,
            )
        except Exception as e:
            print(f"Error: {e}")


@search.command()
@click.argument("display_name", required=True)
@click.option("-c", "--column", multiple=True, required=False)
@click.option("-h", "--honour", is_flag=True)
@click.option("-e", "--exact", is_flag=True)
@click.option("-m", "--multiple", is_flag=True)
@click.option("-d", "--diff", is_flag=True)
def item(
    display_name: str,
    column: tuple[str],
    honour: bool = False,
    exact: bool = False,
    multiple: bool = False,
    diff: bool = False,
):
    translations = spark.read.jdbc(
        url=cfg.db.url,
        table="translations",
    )

    root_templates = spark.read.jdbc(
        url=cfg.db.url,
        table="root_templates",
    ).withColumnRenamed("Name", "templ_name")

    weapons = spark.read.jdbc(
        url=cfg.db.url,
        table="resolved_weapons",
    ).filter(F.col("honour_mode") == honour)

    weapons = weapons.join(
        root_templates,
        root_templates.Stats == weapons.name,
    ).join(translations, translations.contentuid == root_templates.DisplayName, "left")

    armor = spark.read.jdbc(
        url=cfg.db.url,
        table="resolved_armor",
    ).filter(F.col("honour_mode") == honour)

    armor = armor.join(root_templates, root_templates.Stats == armor.name).join(
        translations, translations.contentuid == root_templates.DisplayName, "left"
    )

    if exact:
        found_weapons = weapons.filter(F.col("text") == display_name)
        found_armor = armor.filter(F.col("text") == display_name)
    else:
        found_weapons = weapons.filter(F.col("text").ilike(f"%{display_name}%"))
        found_armor = armor.filter(F.col("text").ilike(f"%{display_name}%"))

    found_weapons_count = found_weapons.count()
    found_armor_count = found_armor.count()
    found_count = found_weapons_count + found_armor_count

    if found_count == 0:
        raise click.ClickException(f"Item with name {display_name} not found.")

    click.echo(f"Found {found_count} items.")

    if found_count == 1:
        item = found_weapons if found_weapons_count == 1 else found_armor
        df = spark.createDataFrame(item.toPandas().dropna(axis=1))

        if len(column) > 0:
            df = df.filter(F.lower(F.col("key")).isin([c.lower() for c in column]))
    else:
        if multiple and not diff:
            if found_weapons_count > 0:
                click.echo("Weapons:")
                spark.createDataFrame(found_weapons.toPandas().dropna(axis=1)).show(
                    found_weapons_count, truncate=False, vertical=True
                )
            if found_armor_count > 0:
                click.echo("Armor:")
                spark.createDataFrame(found_armor.toPandas().dropna(axis=1)).show(
                    found_armor_count, truncate=False, vertical=True
                )
            return
        elif multiple and diff:
            if found_weapons_count > 0:
                click.echo("Weapons:")
                df = found_weapons.toPandas().dropna(axis=1)
                df = df[[i for i in df if df[i].nunique() > 1]]
                spark.createDataFrame(df).show(len(df), truncate=False, vertical=True)
            if found_armor_count > 0:
                click.echo("Armor:")
                df = found_armor.toPandas().dropna(axis=1)
                df = df[[i for i in df if df[i].nunique() > 1]]
                spark.createDataFrame(df).show(len(df), truncate=False, vertical=True)
            return

        df = found_weapons.select(
            F.col("text").alias("name"),
        ).union(
            found_armor.select(
                F.col("text").alias("name"),
            )
        )

    df.show(df.count(), truncate=False, vertical=found_count == 1)


@main.group(name="import")
def import_():
    pass


@import_.command()
def root_templates():
    def find_files(path: str):
        for root, dirs, files in os.walk(path):
            for file in files:
                if "RootTemplates" in root:
                    yield os.path.join(root, file)

    import tempfile
    import shutil
    import xmltodict
    import json

    with tempfile.TemporaryDirectory() as tmpdir:
        files = []
        counter = 0
        for file in find_files(cfg.raw_data_path):
            shutil.copy(file, os.path.join(tmpdir, f"{counter}.xml"))
            files.append(os.path.join(tmpdir, f"{counter}.xml"))
            counter += 1

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
                F.expr("uuid()").alias("id"), F.col("node.attribute").alias("attribute")
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
            table="root_templates",
            mode="overwrite",
        )


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
