import enum
from typing import Dict, List, NamedTuple, Tuple, Optional
import yaml

from pathlib import Path


class SyncConfigDB(NamedTuple):
    host: str
    port: str
    database: str
    user: str
    password: str


class SyncConfigTable(NamedTuple):
    schema: str
    table: str
    columns: Tuple[str, ...]
    pk: Tuple[str, ...]


class SyncConfigMapping(NamedTuple):
    source: SyncConfigTable
    destination: SyncConfigTable


class ETLSyncConfigException(Exception):
    pass


class ENV(enum.Enum):
    TEST = "test"
    DEV = "development"
    PROD = "production"
    LOCAL = "local"

    @staticmethod
    def get_by_value(env: str) -> "ENV":
        _env = env.lower()
        for _value in ENV:
            if _value.value.lower() == _env:
                return _value

        raise ValueError("нет такого значения")


class ETLSyncConfig:
    """Управление конфигурацией ETL-процесса."""
    _destination: SyncConfigDB
    _source: SyncConfigDB
    _mappings: List[SyncConfigMapping]

    def __init__(
            self,
            path: Optional[str] = None,
            env: ENV = ENV.LOCAL,
    ) -> None:
        if path is None:
            self._path = f"{env.name.lower()}.transform.yml"
        else:
            self._path = path

        _config = self._load_environment_config(self._path)

        src = _config.get("source", {})
        if not src:
            raise ETLSyncConfigException("не указан источник")
        self._source = self._db_info(src)

        dst = _config.get("destination", {})
        if not dst:
            raise ETLSyncConfigException("не указан получатель")
        self._destination = self._db_info(dst)

        self._mappings = [
            SyncConfigMapping(
                source=self._table_info(item.get("source", {})),
                destination=self._table_info(item.get("destination", {})),
            )
            for item in _config.get("mappings", [])
        ]

    @property
    def source(self) -> SyncConfigDB:
        return self._source

    @property
    def destination(self) -> SyncConfigDB:
        return self._destination

    @staticmethod
    def _db_info(item: Dict) -> SyncConfigDB:
        return SyncConfigDB(
            host=str(item.get("host", "127.0.0.1")),
            port=str(item.get("post", 5432)),
            database=str(item["database"]),
            user=str(item["user"]),
            password=str(item["password"]),
        )

    @property
    def mappings(self) -> List[SyncConfigMapping]:
        return self._mappings

    @staticmethod
    def _table_info(item: Dict) -> SyncConfigTable:
        columns = item.get("columns", [])
        if not isinstance(columns, list):
            raise ETLSyncConfigException("columns")

        pk = item.get("pk", [])
        if not isinstance(pk, list):
            raise ETLSyncConfigException("pk")

        return SyncConfigTable(
            schema=str(item.get("schema", "public")),
            table=str(item["table"]),
            columns=tuple(map(str, columns)),
            pk=tuple(map(str, pk)),
        )

    @staticmethod
    def _load_environment_config(path: str) -> dict:
        """Загружает конфиг для файла."""
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Конфиг {path} не найден")

        with open(config_path, 'r') as f:
            result = yaml.safe_load(f)

        if not isinstance(result, dict):
            raise ETLSyncConfigException(f"Ошибка формата файла: {path}")

        return result


def main():
    try:
        env = "LocaL"
        etl_config = ETLSyncConfig(env=ENV.get_by_value(env))

        print("\nПример доступа к параметрам:")
        print("source:", etl_config.source)
        print("destination:", etl_config.destination)
        print("DB Host:", etl_config.source.host)

        for item in etl_config.mappings:
            print(f"\nОбработка: {item.source.schema}.{item.source.table}")

        print("Источник:", etl_config.source.database)
        print("Назначение:", etl_config.destination.database)
        print("\nТаблицы для обработки:")
        for idx, mapping in enumerate(etl_config.mappings, 1):
            print(f"{idx}. {mapping.source.schema}.{mapping.source.table} -> "
                  f"{mapping.destination.schema}.{mapping.destination.table}")
            print("\tКолонки:", mapping.source.columns)
            print("\tКолонки:", mapping.destination.columns)
            print("\tКолонки:", mapping.destination.pk)
    except Exception as e:
        print(f"Ошибка загрузки конфигурации: {e}")

if __name__ == "__main__":
    main()
