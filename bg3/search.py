import re
from typing import Optional

import click
from pyspark import SparkContext
from pyspark.sql import functions as F, SparkSession  # noqa

from bg3.config import Config
from bg3.main import main

cfg: Optional[Config] = None
spark: Optional[SparkSession] = None
sc: Optional[SparkContext] = None


@main.group()
@click.pass_context
def search(ctx):
    global cfg
    global spark
    global sc
    cfg = ctx.obj["cfg"]
    spark = ctx.obj["spark"]
    sc = ctx.obj["sc"]


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
        root_templates,
        root_templates.Stats == armor.name,
    ).join(
        translations, translations.contentuid == root_templates.DisplayName, "left"
    ).registerTempTable("armor")

    weapons.join(
        root_templates,
        root_templates.Stats == weapons.name,
    ).join(
        translations, translations.contentuid == root_templates.DisplayName, "left"
    ).registerTempTable("weapons")

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
    )

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
