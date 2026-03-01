from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any, TypeVar

import pyarrow as pa
from databricks import sql

from .settings import AppSettings, AuthMethod

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    import pandas as pd
    import polars as pl
    from databricks.sql.cursor import Cursor

    from .auth import DatabricksAuth


if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

_T = TypeVar("_T")

_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
_MERGE_STAGE = "_merge_stage"


def _arrow_type_to_spark(t: pa.DataType) -> str:
    match t.id:
        case pa.lib.Type_BOOL:
            return "BOOLEAN"
        case pa.lib.Type_INT8:
            return "TINYINT"
        case pa.lib.Type_INT16:
            return "SMALLINT"
        case pa.lib.Type_INT32:
            return "INT"
        case pa.lib.Type_INT64:
            return "BIGINT"
        case pa.lib.Type_FLOAT:
            return "FLOAT"
        case pa.lib.Type_DOUBLE:
            return "DOUBLE"
        case pa.lib.Type_DECIMAL128:
            return f"DECIMAL({t.precision},{t.scale})"  # type: ignore[union-attr]
        case pa.lib.Type_DATE32:
            return "DATE"
        case pa.lib.Type_TIMESTAMP:
            return "TIMESTAMP"
        case pa.lib.Type_BINARY | pa.lib.Type_LARGE_BINARY:
            return "BINARY"
        case _:
            return "STRING"


def _to_arrow(df: Any) -> pa.Table:
    if isinstance(df, pa.Table):
        return df
    if hasattr(df, "to_arrow"):  # polars
        return df.to_arrow()
    return pa.Table.from_pandas(df, preserve_index=False)  # pandas


def compile_named(
    sql_text: str, params: Mapping[str, Any] | None
) -> tuple[str, list[Any]]:
    """Convert :named params to ``?`` positional placeholders.

    List/tuple values are expanded for IN clauses::

        "WHERE id IN :ids"  +  {"ids": [1, 2, 3]}
        →  "WHERE id IN (?, ?, ?)"  +  [1, 2, 3]

    Args:
        sql_text: SQL query text with :named parameters.
        params: Dictionary mapping parameter names to values.

    Returns:
        Tuple of (compiled_sql, values) where compiled_sql has placeholders
        and values is a flat list of parameter values.

    Raises:
        KeyError: If a named parameter in sql_text is missing from params.
    """
    if not params:
        return sql_text, []

    names: list[str] = []

    def repl(m: re.Match[str]) -> str:
        name = m.group(1)
        names.append(name)
        val = params.get(name)
        if isinstance(val, (list, tuple)):
            if not val:
                msg = (
                    f"Parameter '{name}' is an empty list; "
                    "IN () is invalid SQL"
                )
                raise ValueError(msg)
            return f"({', '.join(['?'] * len(val))})"
        return "?"

    compiled = _PARAM_RE.sub(repl, sql_text)

    missing = [n for n in names if n not in params]
    if missing:
        msg = f"Missing SQL parameters: {missing}"
        raise KeyError(msg)

    values: list[Any] = []
    for n in names:
        v = params[n]
        if isinstance(v, (list, tuple)):
            values.extend(v)
        else:
            values.append(v)
    return compiled, values


class SQLClient:
    """Single injected client.

    Supports:
      - PAT authentication (access_token)
      - OAuth U2M (user-to-machine): auth_type="databricks-oauth"
      - Databricks Apps OBO (on-behalf-of-user): access_token supplied per
            request via token_provider

    Notes:
      - Opens a short-lived connection per query (robust against idle
            timeouts).
      - Default catalog/schema are applied via USE statements so SQL files can
            omit them.
    """

    def __init__(self, settings: AppSettings, auth: DatabricksAuth) -> None:
        """Initialize the Databricks client with settings and authentication.

        Args:
            settings: Application settings containing Databricks configuration.
            auth: Authentication credentials and method.
        """
        self._s = settings
        self._a = auth

    def query_polars(
        self, sql_text: str, params: Mapping[str, Any] | None = None
    ) -> pl.DataFrame:
        """Execute a query and return a Polars DataFrame.

        Args:
            sql_text: SQL query text with optional :named parameters.
            params: Named parameters to substitute in sql_text.

        Returns:
            Polars DataFrame containing the query results.

        Raises:
            ImportError: If polars is not installed.
        """
        try:
            import polars as pl  # noqa: PLC0415 - deferred import for optional dependency
        except ImportError as e:
            raise ImportError(
                "polars is required for query_polars(); "
                "run: pip install polars"
            ) from e
        compiled, vals = compile_named(sql_text, params)
        return pl.from_arrow(
            self._run_with_retry(lambda: self._execute(compiled, vals))
        )

    def query_pandas(
        self, sql_text: str, params: Mapping[str, Any] | None = None
    ) -> pd.DataFrame:
        """Execute a query and return a Pandas DataFrame.

        Parameters:
            sql_text: SQL query text with optional :named parameters.
            params: Named parameters to substitute in sql_text.

        Returns:
            Result as a Pandas DataFrame.

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas  # noqa: F401, ICN001, PLC0415
        except ImportError as e:
            raise ImportError(
                "pandas is required for query_pandas(); "
                "run: pip install pandas"
            ) from e
        compiled, vals = compile_named(sql_text, params)
        return self._run_with_retry(
            lambda: self._execute(compiled, vals)
        ).to_pandas()

    def query(
        self, sql_text: str, params: Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return plain Python dicts.

        Bypasses Arrow serialization entirely — fetches rows directly from the
        wire protocol.  Prefer this over ``query_polars``/``query_pandas`` when
        you only need lightweight Python data structures.

        Parameters:
            sql_text: SQL query text with optional :named parameters.
            params: Named parameters to substitute in sql_text.

        Returns:
            List of dictionaries representing query result rows.
        """
        compiled, vals = compile_named(sql_text, params)
        return self._run_with_retry(lambda: self._execute_raw(compiled, vals))

    def merge_dataframe(
        self,
        df: pd.DataFrame | pl.DataFrame,
        target_table: str,
        id_columns: list[str],
        version_column: str | None = None,
    ) -> None:
        """Merge a pandas or polars DataFrame into a Databricks Delta table.

        Converts the DataFrame to a temporary staging table, then executes a
        MERGE INTO statement against ``target_table``.  The configured default
        catalog and schema are applied via USE statements so ``target_table``
        may be a bare name, ``schema.table``, or ``catalog.schema.table``.

        Args:
            df:             pandas or polars DataFrame to write.
            target_table:   Target Delta table name (may be qualified).
            id_columns:     Column(s) used in the MERGE ON join condition.
            version_column: Optional column for optimistic locking.  When
                            supplied the WHEN MATCHED clause additionally
                            checks that the version has not changed since the
                            row was read; rows whose version no longer matches
                            are silently skipped (not overwritten).

        Raises:
            ValueError: If id_columns is empty, contains columns not in the
                DataFrame, or if version_column is specified but not found
                in the DataFrame.
        """
        arrow_table = _to_arrow(df)
        if not id_columns:
            raise ValueError("id_columns must not be empty")
        missing = [c for c in id_columns if c not in arrow_table.schema.names]
        if missing:
            msg = f"id_columns not found in DataFrame: {missing}"
            raise ValueError(msg)
        if version_column and version_column not in arrow_table.schema.names:
            msg = f"version_column '{version_column}' not found in DataFrame"
            raise ValueError(msg)

        # --- DDL for the staging temp table ---
        col_defs = ", ".join(
            f"{f.name} {_arrow_type_to_spark(f.type)}"
            for f in arrow_table.schema
        )
        placeholders = ", ".join(["?"] * len(arrow_table.schema))
        rows = [list(row.values()) for row in arrow_table.to_pylist()]

        # --- MERGE SQL ---
        on_clause = " AND ".join(f"t.{c} = s.{c}" for c in id_columns)
        matched_condition = (
            f" AND t.{version_column} = s.{version_column}"
            if version_column
            else ""
        )
        merge_sql = (
            f"MERGE INTO {target_table} t\n"
            f"USING {_MERGE_STAGE} s\n"
            f"ON {on_clause}\n"
            f"WHEN MATCHED{matched_condition} THEN UPDATE SET *\n"
            f"WHEN NOT MATCHED THEN INSERT *"
        )

        with (
            sql.connect(**self._build_conn_kwargs()) as conn,
            conn.cursor() as cur,
        ):
            self._apply_context(cur)
            cur.execute(
                f"CREATE OR REPLACE TEMP TABLE {_MERGE_STAGE} ({col_defs})"
            )
            cur.executemany(
                f"INSERT INTO {_MERGE_STAGE} VALUES ({placeholders})", rows
            )
            cur.execute(merge_sql)

    def _run_with_retry(self, fn: Callable[[], _T]) -> _T:
        attempts = self._s.databricks_retry_attempts + 1
        last_err: Exception | None = None

        for attempt in range(attempts):
            try:
                return fn()
            except Exception as e:  # noqa: BLE001 — broad catch required for retry logic
                last_err = e
                if attempt >= attempts - 1:
                    break
                time.sleep(self._s.databricks_retry_backoff_s * (2**attempt))

        if last_err is None:
            msg = "No attempts were made"
            raise RuntimeError(msg)
        raise last_err

    def _execute(self, compiled_sql: str, vals: Sequence[Any]) -> pa.Table:
        with (
            sql.connect(**self._build_conn_kwargs()) as conn,
            conn.cursor() as cur,
        ):
            self._apply_context(cur)
            if self._s.query_tag:
                compiled_sql = f"/* {self._s.query_tag} */\n{compiled_sql}"
            cur.execute(compiled_sql, list(vals) if vals else None)
            return cur.fetchall_arrow()

    def _execute_raw(
        self, compiled_sql: str, vals: Sequence[Any]
    ) -> list[dict[str, Any]]:
        with (
            sql.connect(**self._build_conn_kwargs()) as conn,
            conn.cursor() as cur,
        ):
            self._apply_context(cur)
            if self._s.query_tag:
                compiled_sql = f"/* {self._s.query_tag} */\n{compiled_sql}"
            cur.execute(compiled_sql, list(vals) if vals else None)
            cols = [d[0] for d in cur.description] if cur.description else []
            return [
                dict(zip(cols, row, strict=True)) for row in cur.fetchall()
            ]

    def _build_conn_kwargs(self) -> dict[str, Any]:
        conn_kwargs: dict[str, Any] = {
            "server_hostname": self._s.databricks_server_hostname,
            "http_path": self._s.databricks_http_path,
            "timeout": self._s.databricks_connect_timeout_s,
        }
        if self._a.method == AuthMethod.PAT:
            conn_kwargs["access_token"] = self._a.access_token
        elif self._a.method == AuthMethod.U2M:
            conn_kwargs["auth_type"] = "databricks-oauth"
            if self._a.credentials_provider:
                conn_kwargs["credentials_provider"] = (
                    self._a.credentials_provider
                )
            elif self._a.oauth_persistence:
                conn_kwargs["experimental_oauth_persistence"] = (
                    self._a.oauth_persistence
                )
        elif self._a.method == AuthMethod.U2M_PERSISTENT:
            # SDK-backed credentials provider; cfg.authenticate returns fresh
            # headers on every call via the SDK's Refreshable token source.
            conn_kwargs["credentials_provider"] = self._a.credentials_provider
        elif self._a.method == AuthMethod.OBO:
            if not self._a.token_provider:
                raise ValueError(
                    "OBO authentication requires DatabricksAuth."
                    "token_provider. Inject it from Streamlit "
                    "(st.context.headers)."
                )
            conn_kwargs["access_token"] = self._a.token_provider()
        else:
            msg = f"Unsupported auth method: {self._a.method}"
            raise NotImplementedError(msg)
        return conn_kwargs

    def _apply_context(self, cur: Cursor) -> None:
        if self._s.databricks_default_catalog:
            cur.execute(f"USE CATALOG {self._s.databricks_default_catalog}")
        if self._s.databricks_default_schema:
            cur.execute(f"USE SCHEMA {self._s.databricks_default_schema}")
