from dataclasses import dataclass
from importlib import resources


@dataclass(frozen=True)
class SqlQuery:
    name: str
    sql: str


class QueryRegistry:
    def __init__(self, package: str = "app.queries"):
        self._package = package
        self._cache: dict[str, SqlQuery] = {}

    def get(self, name: str) -> SqlQuery:
        if name not in self._cache:
            path = f"{name}.sql"
            text = (
                resources.files(self._package)
                .joinpath(path)
                .read_text("utf-8")
            )
            self._cache[name] = SqlQuery(name, text.strip() + "\n")
        return self._cache[name]
