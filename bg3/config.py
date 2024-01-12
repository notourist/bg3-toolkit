import logging
import os
from dataclasses import dataclass, fields, is_dataclass, field
from typing import Optional, get_type_hints

import yaml

logger = logging.getLogger(__name__)


@dataclass
class DBConfig:
    host: Optional[str] = "localhost"
    port: Optional[int] = 5432
    database: str = None
    user: str = None
    password: str = None
    schema: str = None
    url: Optional[str] = None
    database_type: Optional[str] = "postgresql"  # in jdbc format

    def __post_init__(self):
        if self.url is None:
            self.url = (
                f"jdbc:{self.database_type}://{self.host}:{self.port}/{self.database}"
                f"?user={self.user}&password={self.password}&currentSchema={self.schema}"
            )


@dataclass
class SparkConfig:
    packages: Optional[list[str]] = field(default_factory=list)
    jars: Optional[list[str]] = field(default_factory=list)
    jdbc_driver_path: str = None
    memory: Optional[str] = "4g"

    def __post_init__(self):
        if self.jdbc_driver_path is not None:
            self.jars.append(self.jdbc_driver_path)


@dataclass
class Config:
    db: DBConfig = field(default_factory=dict)
    spark: SparkConfig = field(default_factory=dict)
    raw_data_path: Optional[str] = None

    def __post_init__(self):
        def init_nested(instance):
            for f in fields(instance):
                if isinstance(getattr(instance, f.name, None), dict) and is_dataclass(
                    f.type
                ):
                    setattr(instance, f.name, f.type(**getattr(instance, f.name)))
                    init_nested(f.type)

        init_nested(self)

    @staticmethod
    def load(path: str) -> "Config":
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found at {path}")

        with open(path, "r") as f:
            config = Config(**yaml.safe_load(f))

        def check_fields(instance):
            types = get_type_hints(instance)
            for field in fields(instance):
                if (
                    not getattr(instance, field.name)
                    and types.get(field.name) is not Optional  # TODO: fix this
                ):
                    logger.warning(f"Missing config value for {field.name}")

        check_fields(config)
        check_fields(config.db)
        check_fields(config.spark)

        return config
