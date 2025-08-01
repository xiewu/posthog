import asyncio
import collections.abc
import contextlib
import csv
import dataclasses
import datetime as dt
import json
import re
import typing

import psycopg
import pyarrow as pa
from django.conf import settings
from psycopg import sql
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from posthog.batch_exports.models import BatchExportRun
from posthog.batch_exports.service import (
    BatchExportField,
    BatchExportInsertInputs,
    BatchExportModel,
    PostgresBatchExportInputs,
)
from posthog.temporal.batch_exports.batch_exports import (
    FinishBatchExportRunInputs,
    RecordsCompleted,
    StartBatchExportRunInputs,
    default_fields,
    execute_batch_export_insert_activity,
    get_data_interval,
    start_batch_export_run,
)
from posthog.temporal.batch_exports.heartbeat import (
    BatchExportRangeHeartbeatDetails,
    DateRange,
    should_resume_from_activity_heartbeat,
)
from posthog.temporal.batch_exports.spmc import (
    Consumer,
    Producer,
    RecordBatchQueue,
    resolve_batch_exports_model,
    run_consumer,
    wait_for_schema_or_producer,
)
from posthog.temporal.batch_exports.temporary_file import (
    BatchExportTemporaryFile,
    WriterFormat,
)
from posthog.temporal.batch_exports.utils import (
    JsonType,
    make_retryable_with_exponential_backoff,
    set_status_to_running_task,
)
from posthog.temporal.common.base import PostHogWorkflow
from posthog.temporal.common.heartbeat import Heartbeater
from posthog.temporal.common.logger import bind_temporal_worker_logger

PostgreSQLField = tuple[str, typing.LiteralString]
Fields = collections.abc.Iterable[PostgreSQLField]

# Compiled regex patterns for PostgreSQL data cleaning
NULL_UNICODE_PATTERN = re.compile(rb"(?<!\\)\\u0000")
UNPAIRED_SURROGATE_PATTERN = re.compile(
    rb"(\\u[dD][89A-Fa-f][0-9A-Fa-f]{2}\\u[dD][c-fC-F][0-9A-Fa-f]{2})|(\\u[dD][89A-Fa-f][0-9A-Fa-f]{2})"
)
UNPAIRED_SURROGATE_PATTERN_2 = re.compile(
    rb"(\\u[dD][89A-Fa-f][0-9A-Fa-f]{2}\\u[dD][c-fC-F][0-9A-Fa-f]{2})|(\\u[dD][c-fC-F][0-9A-Fa-f]{2})"
)


class PostgreSQLConnectionError(Exception):
    pass


class MissingPrimaryKeyError(Exception):
    def __init__(self, table: sql.Identifier, primary_key: sql.Composed):
        super().__init__(f"An operation could not be completed as '{table}' is missing a primary key on {primary_key}")


@dataclasses.dataclass(kw_only=True)
class PostgresInsertInputs(BatchExportInsertInputs):
    """Inputs for Postgres."""

    user: str
    password: str
    host: str
    port: int = 5432
    database: str
    schema: str = "public"
    table_name: str
    has_self_signed_cert: bool = False


class PostgreSQLClient:
    """PostgreSQL connection client used in batch exports."""

    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        database: str,
        has_self_signed_cert: bool,
        connection_timeout: int = 30,
    ):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.has_self_signed_cert = has_self_signed_cert
        self.connection_timeout = connection_timeout

        self._connection: None | psycopg.AsyncConnection = None

    @classmethod
    def from_inputs(cls, inputs: PostgresInsertInputs) -> typing.Self:
        """Initialize `PostgreSQLClient` from `PostgresInsertInputs`."""
        return cls(
            user=inputs.user,
            password=inputs.password,
            database=inputs.database,
            host=inputs.host,
            port=inputs.port,
            has_self_signed_cert=inputs.has_self_signed_cert,
        )

    @property
    def connection(self) -> psycopg.AsyncConnection:
        """Raise if a `psycopg.AsyncConnection` hasn't been established, else return it."""
        if self._connection is None:
            raise PostgreSQLConnectionError("Not connected, open a connection by calling connect")
        return self._connection

    @contextlib.asynccontextmanager
    async def connect(
        self,
    ) -> typing.AsyncIterator[typing.Self]:
        """Manage a PostgreSQL connection.

        By using a context manager Pyscopg will take care of closing the connection.
        """
        kwargs: dict[str, typing.Any] = {}
        if self.has_self_signed_cert:
            # Disable certificate verification for self-signed certificates.
            kwargs["sslrootcert"] = None

        max_attempts = 5
        connect: typing.Callable[..., typing.Awaitable[psycopg.AsyncConnection]] = (
            make_retryable_with_exponential_backoff(
                psycopg.AsyncConnection.connect,
                max_attempts=max_attempts,
                retryable_exceptions=(psycopg.OperationalError, psycopg.errors.ConnectionTimeout),
            )
        )

        try:
            connection: psycopg.AsyncConnection = await connect(
                user=self.user,
                password=self.password,
                dbname=self.database,
                host=self.host,
                port=self.port,
                connect_timeout=self.connection_timeout,
                sslmode="prefer" if settings.TEST else "require",
                **kwargs,
            )
        except psycopg.errors.ConnectionTimeout as err:
            raise PostgreSQLConnectionError(
                f"Timed-out while trying to connect for {max_attempts} attempts. Is the "
                f"server running at '{self.host}', port '{self.port}' and accepting "
                "TCP/IP connections?"
            ) from err
        except psycopg.OperationalError as err:
            raise PostgreSQLConnectionError(
                f"Failed to connect after {max_attempts} attempts. Please review connection configuration."
            ) from err

        async with connection as connection:
            self._connection = connection
            yield self

    async def acreate_table(
        self,
        schema: str | None,
        table_name: str,
        fields: Fields,
        exists_ok: bool = True,
        primary_key: Fields | None = None,
    ) -> None:
        """Create a table in PostgreSQL.

        Args:
            schema: Name of the schema where the table is to be created.
            table_name: Name of the table to create.
            fields: An iterable of PostgreSQL fields for the table.
            exists_ok: Whether to ignore if the table already exists.
            primary_key: Optionally set a primary key on these fields, needed for merges.
        """
        if schema:
            table_identifier = sql.Identifier(schema, table_name)
        else:
            table_identifier = sql.Identifier(table_name)

        if exists_ok is True:
            base_query = "CREATE TABLE IF NOT EXISTS {table} ({fields}{pkey})"
        else:
            base_query = "CREATE TABLE {table} ({fields}{pkey})"

        if primary_key is not None:
            primary_key_clause = sql.SQL(", PRIMARY KEY ({fields})").format(
                fields=sql.SQL(",").join(sql.Identifier(field[0]) for field in primary_key)
            )

        async with self.connection.transaction():
            async with self.connection.cursor() as cursor:
                await cursor.execute("SET TRANSACTION READ WRITE")

                await cursor.execute(
                    sql.SQL(base_query).format(
                        pkey=primary_key_clause if primary_key else sql.SQL(""),
                        table=table_identifier,
                        fields=sql.SQL(",").join(
                            sql.SQL("{field} {type}").format(
                                field=sql.Identifier(field),
                                type=sql.SQL(field_type),
                            )
                            for field, field_type in fields
                        ),
                    )
                )

    async def adelete_table(self, schema: str | None, table_name: str, not_found_ok: bool = True) -> None:
        """Delete a table in PostgreSQL.

        Args:
            schema: Name of the schema where the table to delete is located.
            table_name: Name of the table to delete.
            not_found_ok: Whether to ignore if the table doesn't exist.
        """
        if schema:
            table_identifier = sql.Identifier(schema, table_name)
        else:
            table_identifier = sql.Identifier(table_name)

        if not_found_ok is True:
            base_query = "DROP TABLE IF EXISTS {table}"
        else:
            base_query = "DROP TABLE {table}"

        async with self.connection.transaction():
            async with self.connection.cursor() as cursor:
                await cursor.execute("SET TRANSACTION READ WRITE")

                await cursor.execute(sql.SQL(base_query).format(table=table_identifier))

    async def aget_table_columns(self, schema: str | None, table_name: str) -> list[str]:
        """Get the column names for a table in PostgreSQL.

        Args:
            schema: Name of the schema where the table is located.
            table_name: Name of the table to get columns for.

        Returns:
            A list of column names in the table.
        """
        if schema:
            table_identifier = sql.Identifier(schema, table_name)
        else:
            table_identifier = sql.Identifier(table_name)

        async with self.connection.transaction():
            async with self.connection.cursor() as cursor:
                await cursor.execute(sql.SQL("SELECT * FROM {} WHERE 1=0").format(table_identifier))
                columns = [column.name for column in cursor.description or []]
                return columns

    @contextlib.asynccontextmanager
    async def managed_table(
        self,
        schema: str,
        table_name: str,
        fields: Fields,
        primary_key: Fields | None = None,
        exists_ok: bool = True,
        not_found_ok: bool = True,
        delete: bool = True,
        create: bool = True,
    ) -> collections.abc.AsyncGenerator[str, None]:
        """Manage a table in PostgreSQL by ensure it exists while in context.

        Managing a table implies two operations: creation of a table, which happens upon entering the
        context manager, and deletion of the table, which happens upon exiting.

        Args:
            schema: Schema where the managed table is.
            table_name: A name for the managed table.
            fields: An iterable of PostgreSQL fields for the table when it has to be created.
            primary_key: Optionally set a primary key on these fields on creation.
            exists_ok: Whether to ignore if the table already exists on creation.
            not_found_ok: Whether to ignore if the table doesn't exist.
            delete: If `False`, do not delete the table on exiting context manager.
            create: If `False`, do not attempt to create the table.
        """
        if create is True:
            await self.acreate_table(schema, table_name, fields, exists_ok, primary_key=primary_key)

        try:
            yield table_name
        finally:
            if delete is True:
                await self.adelete_table(schema, table_name, not_found_ok)

    async def amerge_mutable_tables(
        self,
        final_table_name: str,
        stage_table_name: str,
        schema: str,
        merge_key: Fields,
        update_key: Fields,
        update_when_matched: Fields,
    ) -> None:
        """Merge two identical person model tables in PostgreSQL.

        Merging utilizes PostgreSQL's `INSERT INTO ... ON CONFLICT` statement. PostgreSQL version
        15 and later supports a `MERGE` command, but to ensure support for older versions of PostgreSQL
        we do not use it. There are differences in the way concurrency is managed in `MERGE` but those
        are less relevant concerns for us than compatibility.
        """
        if schema:
            final_table_identifier = sql.Identifier(schema, final_table_name)
            stage_table_identifier = sql.Identifier(schema, stage_table_name)

        else:
            final_table_identifier = sql.Identifier(final_table_name)
            stage_table_identifier = sql.Identifier(stage_table_name)

        and_separator = sql.SQL(" AND ")
        merge_condition = and_separator.join(
            sql.SQL("{final_field} = {stage_field}").format(
                final_field=sql.Identifier("final", field[0]),
                stage_field=sql.Identifier(schema, stage_table_name, field[0]),
            )
            for field in merge_key
        )

        or_separator = sql.SQL(" OR ")
        update_condition = or_separator.join(
            sql.SQL("EXCLUDED.{stage_field} > final.{final_field}").format(
                final_field=sql.Identifier(field[0]),
                stage_field=sql.Identifier(field[0]),
            )
            for field in update_key
        )

        comma = sql.SQL(",")
        update_clause = comma.join(
            sql.SQL("{final_field} = EXCLUDED.{stage_field}").format(
                final_field=sql.Identifier(field[0]),
                stage_field=sql.Identifier(field[0]),
            )
            for field in update_when_matched
        )
        field_names = comma.join(sql.Identifier(field[0]) for field in update_when_matched)
        conflict_fields = comma.join(sql.Identifier(field[0]) for field in merge_key)

        merge_query = sql.SQL(
            """\
        INSERT INTO {final_table} AS final ({field_names})
        SELECT {field_names} FROM {stage_table}
        ON CONFLICT ({conflict_fields}) DO UPDATE SET
            {update_clause}
        WHERE ({update_condition})
        """
        ).format(
            final_table=final_table_identifier,
            conflict_fields=conflict_fields,
            stage_table=stage_table_identifier,
            merge_condition=merge_condition,
            update_condition=update_condition,
            update_clause=update_clause,
            field_names=field_names,
        )

        async with self.connection.transaction():
            async with self.connection.cursor() as cursor:
                if schema:
                    await cursor.execute(sql.SQL("SET search_path TO {schema}").format(schema=sql.Identifier(schema)))
                await cursor.execute("SET TRANSACTION READ WRITE")

                try:
                    await cursor.execute(merge_query)
                except psycopg.errors.InvalidColumnReference:
                    raise MissingPrimaryKeyError(final_table_identifier, conflict_fields)

    async def copy_tsv_to_postgres(
        self,
        tsv_file,
        schema: str,
        table_name: str,
        schema_columns: list[str],
    ) -> None:
        """Execute a COPY FROM query with given connection to copy contents of tsv_file.

        Arguments:
            tsv_file: A file-like object to interpret as TSV to copy its contents.
            schema: The schema where the table we are COPYing into exists.
            table_name: The name of the table we are COPYing into.
            schema_columns: The column names of the table we are COPYing into.
        """
        tsv_file.seek(0)

        async with self.connection.transaction():
            async with self.connection.cursor() as cursor:
                if schema:
                    await cursor.execute(sql.SQL("SET search_path TO {schema}").format(schema=sql.Identifier(schema)))

                await cursor.execute("SET TRANSACTION READ WRITE")

                async with cursor.copy(
                    # TODO: Switch to binary encoding as CSV has a million edge cases.
                    sql.SQL("COPY {table_name} ({fields}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')").format(
                        table_name=sql.Identifier(table_name),
                        fields=sql.SQL(",").join(sql.Identifier(column) for column in schema_columns),
                    )
                ) as copy:
                    while data := await asyncio.to_thread(tsv_file.read):
                        data = remove_invalid_json(data)
                        await copy.write(data)


def remove_invalid_json(data: bytes) -> bytes:
    """Remove invalid JSON from a byte string."""
    # \u0000 cannot be present in PostgreSQL's jsonb type, and will cause an error.
    # See: https://www.postgresql.org/docs/17/datatype-json.html
    # We use a regex to avoid replacing escaped \u0000 (for example, \\u0000, which we have seen in
    # some actual data)
    data = NULL_UNICODE_PATTERN.sub(b"", data)
    # Remove unpaired unicode surrogates
    data = UNPAIRED_SURROGATE_PATTERN.sub(rb"\1", data)
    data = UNPAIRED_SURROGATE_PATTERN_2.sub(rb"\1", data)
    return data


def postgres_default_fields() -> list[BatchExportField]:
    batch_export_fields = default_fields()
    batch_export_fields.append(
        {
            "expression": "nullIf(JSONExtractString(properties, '$ip'), '')",
            "alias": "ip",
        }
    )
    # Fields kept or removed for backwards compatibility with legacy apps schema.
    batch_export_fields.append({"expression": "toJSONString(toJSONString(elements_chain))", "alias": "elements"})
    batch_export_fields.append({"expression": "Null::Nullable(String)", "alias": "site_url"})
    batch_export_fields.pop(batch_export_fields.index({"expression": "created_at", "alias": "created_at"}))
    # Team ID is (for historical reasons) an INTEGER (4 bytes) in PostgreSQL, but in ClickHouse is stored as Int64.
    # We can't encode it as an Int64, as this includes 4 extra bytes, and PostgreSQL will reject the data with a
    # 'incorrect binary data format' error on the column, so we cast it to Int32.
    team_id_field = batch_export_fields.pop(
        batch_export_fields.index(BatchExportField(expression="team_id", alias="team_id"))
    )
    team_id_field["expression"] = "toInt32(team_id)"
    batch_export_fields.append(team_id_field)
    return batch_export_fields


def get_postgres_fields_from_record_schema(
    record_schema: pa.Schema, known_json_columns: list[str]
) -> list[PostgreSQLField]:
    """Generate a list of supported PostgreSQL fields from PyArrow schema.

    This function is used to map custom schemas to PostgreSQL-supported types. Some loss of precision is
    expected.
    """
    pg_schema: list[PostgreSQLField] = []

    for name in record_schema.names:
        pa_field = record_schema.field(name)

        if pa.types.is_string(pa_field.type) or isinstance(pa_field.type, JsonType):
            if pa_field.name in known_json_columns:
                pg_type = "JSONB"
            else:
                pg_type = "TEXT"

        elif pa.types.is_signed_integer(pa_field.type) or pa.types.is_unsigned_integer(pa_field.type):
            if pa.types.is_uint64(pa_field.type) or pa.types.is_int64(pa_field.type):
                pg_type = "BIGINT"
            else:
                pg_type = "INTEGER"

        elif pa.types.is_floating(pa_field.type):
            if pa.types.is_float64(pa_field.type):
                pg_type = "DOUBLE PRECISION"
            else:
                pg_type = "REAL"

        elif pa.types.is_boolean(pa_field.type):
            pg_type = "BOOLEAN"

        elif pa.types.is_timestamp(pa_field.type):
            if pa_field.type.tz is not None:
                pg_type = "TIMESTAMPTZ"
            else:
                pg_type = "TIMESTAMP"

        elif pa.types.is_list(pa_field.type) and pa.types.is_string(pa_field.type.value_type):
            pg_type = "TEXT[]"

        else:
            raise TypeError(f"Unsupported type in field '{name}': '{pa_field.type}'")

        pg_schema.append((name, pg_type))

    return pg_schema


@dataclasses.dataclass
class PostgreSQLHeartbeatDetails(BatchExportRangeHeartbeatDetails):
    """The PostgreSQL batch export details included in every heartbeat."""

    pass


class PostgreSQLConsumer(Consumer):
    def __init__(
        self,
        heartbeater: Heartbeater,
        heartbeat_details: PostgreSQLHeartbeatDetails,
        data_interval_start: dt.datetime | str | None,
        data_interval_end: dt.datetime | str,
        writer_format: WriterFormat,
        postgresql_client: PostgreSQLClient,
        postgresql_table: str,
        postgresql_table_schema: str,
        postgresql_table_fields: list[str],
    ):
        super().__init__(
            heartbeater=heartbeater,
            heartbeat_details=heartbeat_details,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            writer_format=writer_format,
        )
        self.heartbeat_details: PostgreSQLHeartbeatDetails = heartbeat_details
        self.postgresql_table = postgresql_table
        self.postgresql_table_schema = postgresql_table_schema
        self.postgresql_table_fields = postgresql_table_fields
        self.postgresql_client = postgresql_client

    async def flush(
        self,
        batch_export_file: BatchExportTemporaryFile,
        records_since_last_flush: int,
        bytes_since_last_flush: int,
        flush_counter: int,
        last_date_range: DateRange,
        is_last: bool,
        error: Exception | None,
    ):
        await self.logger.adebug(
            "Copying %s records of size %s bytes",
            records_since_last_flush,
            bytes_since_last_flush,
        )

        await self.postgresql_client.copy_tsv_to_postgres(
            batch_export_file,
            self.postgresql_table_schema,
            self.postgresql_table,
            self.postgresql_table_fields,
        )

        await self.logger.ainfo("Copied %s to PostgreSQL table '%s'", records_since_last_flush, self.postgresql_table)
        self.rows_exported_counter.add(records_since_last_flush)
        self.bytes_exported_counter.add(bytes_since_last_flush)

        self.heartbeat_details.records_completed += records_since_last_flush
        self.heartbeat_details.track_done_range(last_date_range, self.data_interval_start)


@activity.defn
async def insert_into_postgres_activity(inputs: PostgresInsertInputs) -> RecordsCompleted:
    """Activity streams data from ClickHouse to Postgres."""
    logger = await bind_temporal_worker_logger(team_id=inputs.team_id, destination="PostgreSQL")
    await logger.ainfo(
        "Batch exporting range %s - %s to PostgreSQL: %s.%s.%s",
        inputs.data_interval_start or "START",
        inputs.data_interval_end or "END",
        inputs.database,
        inputs.schema,
        inputs.table_name,
    )

    async with (
        Heartbeater() as heartbeater,
        set_status_to_running_task(run_id=inputs.run_id, logger=logger),
    ):
        _, details = await should_resume_from_activity_heartbeat(activity, PostgreSQLHeartbeatDetails)
        if details is None:
            details = PostgreSQLHeartbeatDetails()

        done_ranges: list[DateRange] = details.done_ranges

        model, record_batch_model, model_name, fields, filters, extra_query_parameters = resolve_batch_exports_model(
            inputs.team_id, inputs.batch_export_model, inputs.batch_export_schema
        )

        data_interval_start = (
            dt.datetime.fromisoformat(inputs.data_interval_start) if inputs.data_interval_start else None
        )
        data_interval_end = dt.datetime.fromisoformat(inputs.data_interval_end)
        full_range = (data_interval_start, data_interval_end)

        queue = RecordBatchQueue(max_size_bytes=settings.BATCH_EXPORT_POSTGRES_RECORD_BATCH_QUEUE_MAX_SIZE_BYTES)
        producer = Producer(record_batch_model)
        producer_task = await producer.start(
            queue=queue,
            model_name=model_name,
            is_backfill=inputs.get_is_backfill(),
            backfill_details=inputs.backfill_details,
            team_id=inputs.team_id,
            full_range=full_range,
            done_ranges=done_ranges,
            fields=fields,
            filters=filters,
            destination_default_fields=postgres_default_fields(),
            exclude_events=inputs.exclude_events,
            include_events=inputs.include_events,
            extra_query_parameters=extra_query_parameters,
        )

        record_batch_schema = await wait_for_schema_or_producer(queue, producer_task)
        if record_batch_schema is None:
            return details.records_completed

        record_batch_schema = pa.schema(
            [field.with_nullable(True) for field in record_batch_schema if field.name != "_inserted_at"]
        )

        if model is None or (isinstance(model, BatchExportModel) and model.name == "events"):
            table_fields: Fields = [
                ("uuid", "VARCHAR(200)"),
                ("event", "VARCHAR(200)"),
                ("properties", "JSONB"),
                ("elements", "JSONB"),
                ("set", "JSONB"),
                ("set_once", "JSONB"),
                ("distinct_id", "VARCHAR(200)"),
                ("team_id", "INTEGER"),
                ("ip", "VARCHAR(200)"),
                ("site_url", "VARCHAR(200)"),
                ("timestamp", "TIMESTAMP WITH TIME ZONE"),
            ]

        else:
            table_fields = get_postgres_fields_from_record_schema(
                record_batch_schema,
                known_json_columns=["properties", "set", "set_once", "person_properties"],
            )

        requires_merge = False
        merge_key: Fields = []
        update_key: Fields = []
        primary_key: Fields | None = None
        if isinstance(inputs.batch_export_model, BatchExportModel):
            if inputs.batch_export_model.name == "persons":
                requires_merge = True
                merge_key = [
                    ("team_id", "INT"),
                    ("distinct_id", "TEXT"),
                ]
                update_key = [
                    ("person_version", "INT"),
                    ("person_distinct_id_version", "INT"),
                ]
                primary_key = (("team_id", "INTEGER"), ("distinct_id", "VARCHAR(200)"))

            elif inputs.batch_export_model.name == "sessions":
                requires_merge = True
                merge_key = [
                    ("team_id", "INT"),
                    ("session_id", "TEXT"),
                ]
                update_key = [
                    ("end_timestamp", "TIMESTAMP"),
                ]
                primary_key = (("team_id", "INTEGER"), ("session_id", "TEXT"))

        data_interval_end_str = dt.datetime.fromisoformat(inputs.data_interval_end).strftime("%Y-%m-%d_%H-%M-%S")
        # NOTE: PostgreSQL has a 63 byte limit on identifiers.
        # With a 6 digit `team_id`, this leaves 30 bytes for a table name input.
        # TODO: That should be enough, but we should add a proper check and alert on larger inputs.
        stagle_table_name = (
            f"stage_{inputs.table_name}_{data_interval_end_str}_{inputs.team_id}"
            if requires_merge
            else inputs.table_name
        )[:63]

        async with PostgreSQLClient.from_inputs(inputs).connect() as pg_client:
            # handle the case where the final table doesn't contain all the fields present in the record batch schema
            try:
                columns = await pg_client.aget_table_columns(inputs.schema, inputs.table_name)
                table_fields = [field for field in table_fields if field[0] in columns]
            except psycopg.errors.InsufficientPrivilege:
                await logger.awarning(
                    "Insufficient privileges to get table columns for table '%s.%s'; "
                    "will assume all columns are present. If this results in an error, please grant SELECT "
                    "permissions on this table or ensure the destination table is using the latest schema "
                    "as described in the docs: https://posthog.com/docs/cdp/batch-exports/postgres",
                    inputs.schema,
                    inputs.table_name,
                )
            except psycopg.errors.UndefinedTable:
                # this can happen if the table doesn't exist yet
                pass

            schema_columns = [field[0] for field in table_fields]

            async with (
                pg_client.managed_table(
                    inputs.schema, inputs.table_name, table_fields, delete=False, primary_key=primary_key
                ) as pg_table,
                pg_client.managed_table(
                    inputs.schema,
                    stagle_table_name,
                    table_fields,
                    create=requires_merge,
                    delete=requires_merge,
                    primary_key=primary_key,
                ) as pg_stage_table,
            ):
                consumer = PostgreSQLConsumer(
                    heartbeater=heartbeater,
                    heartbeat_details=details,
                    data_interval_end=data_interval_end,
                    data_interval_start=data_interval_start,
                    writer_format=WriterFormat.CSV,
                    postgresql_client=pg_client,
                    postgresql_table=pg_stage_table if requires_merge else pg_table,
                    postgresql_table_schema=inputs.schema,
                    postgresql_table_fields=schema_columns,
                )
                try:
                    _ = await run_consumer(
                        consumer=consumer,
                        queue=queue,
                        producer_task=producer_task,
                        schema=record_batch_schema,
                        max_bytes=settings.BATCH_EXPORT_POSTGRES_UPLOAD_CHUNK_SIZE_BYTES,
                        json_columns=(),
                        writer_file_kwargs={
                            "delimiter": "\t",
                            "quoting": csv.QUOTE_MINIMAL,
                            "escape_char": None,
                            "field_names": schema_columns,
                        },
                        multiple_files=True,
                    )
                finally:
                    if requires_merge:
                        await pg_client.amerge_mutable_tables(
                            final_table_name=pg_table,
                            stage_table_name=pg_stage_table,
                            schema=inputs.schema,
                            update_when_matched=table_fields,
                            merge_key=merge_key,
                            update_key=update_key,
                        )

                return details.records_completed


@workflow.defn(name="postgres-export", failure_exception_types=[workflow.NondeterminismError])
class PostgresBatchExportWorkflow(PostHogWorkflow):
    """A Temporal Workflow to export ClickHouse data into Postgres.

    This Workflow is intended to be executed both manually and by a Temporal
    Schedule. When ran by a schedule, `data_interval_end` should be set to
    `None` so that we will fetch the end of the interval from the Temporal
    search attribute `TemporalScheduledStartTime`.
    """

    @staticmethod
    def parse_inputs(inputs: list[str]) -> PostgresBatchExportInputs:
        """Parse inputs from the management command CLI."""
        loaded = json.loads(inputs[0])
        return PostgresBatchExportInputs(**loaded)

    @workflow.run
    async def run(self, inputs: PostgresBatchExportInputs):
        """Workflow implementation to export data to Postgres."""
        is_backfill = inputs.get_is_backfill()
        is_earliest_backfill = inputs.get_is_earliest_backfill()
        data_interval_start, data_interval_end = get_data_interval(inputs.interval, inputs.data_interval_end)
        should_backfill_from_beginning = is_backfill and is_earliest_backfill

        start_batch_export_run_inputs = StartBatchExportRunInputs(
            team_id=inputs.team_id,
            batch_export_id=inputs.batch_export_id,
            data_interval_start=data_interval_start.isoformat() if not should_backfill_from_beginning else None,
            data_interval_end=data_interval_end.isoformat(),
            exclude_events=inputs.exclude_events,
            include_events=inputs.include_events,
            backfill_id=inputs.backfill_details.backfill_id if inputs.backfill_details else None,
        )
        run_id = await workflow.execute_activity(
            start_batch_export_run,
            start_batch_export_run_inputs,
            start_to_close_timeout=dt.timedelta(minutes=5),
            retry_policy=RetryPolicy(
                initial_interval=dt.timedelta(seconds=10),
                maximum_interval=dt.timedelta(seconds=60),
                maximum_attempts=0,
                non_retryable_error_types=["NotNullViolation", "IntegrityError"],
            ),
        )

        finish_inputs = FinishBatchExportRunInputs(
            id=run_id,
            batch_export_id=inputs.batch_export_id,
            status=BatchExportRun.Status.COMPLETED,
            team_id=inputs.team_id,
        )

        insert_inputs = PostgresInsertInputs(
            team_id=inputs.team_id,
            user=inputs.user,
            password=inputs.password,
            host=inputs.host,
            port=inputs.port,
            database=inputs.database,
            schema=inputs.schema,
            table_name=inputs.table_name,
            has_self_signed_cert=inputs.has_self_signed_cert,
            data_interval_start=data_interval_start.isoformat() if not should_backfill_from_beginning else None,
            data_interval_end=data_interval_end.isoformat(),
            exclude_events=inputs.exclude_events,
            include_events=inputs.include_events,
            run_id=run_id,
            backfill_details=inputs.backfill_details,
            is_backfill=is_backfill,
            batch_export_model=inputs.batch_export_model,
            batch_export_schema=inputs.batch_export_schema,
        )

        await execute_batch_export_insert_activity(
            insert_into_postgres_activity,
            insert_inputs,
            interval=inputs.interval,
            non_retryable_error_types=[
                # Raised on errors that are related to database operation.
                # For example: unexpected disconnect, database or other object not found.
                "OperationalError",
                # The schema name provided is invalid (usually because it doesn't exist).
                "InvalidSchemaName",
                # Missing permissions to, e.g., insert into table.
                "InsufficientPrivilege",
                # Issue with exported data compared to schema, retrying won't help.
                "NotNullViolation",
                # A user added a unique constraint on their table, but batch exports (particularly events)
                # can cause duplicates.
                "UniqueViolation",
                # Something changed in the target table's schema that we were not expecting.
                "UndefinedColumn",
                # A VARCHAR column is too small.
                "StringDataRightTruncation",
                # Raised by PostgreSQL client. Self explanatory.
                "DiskFull",
                # Raised by our PostgreSQL client when failing to connect after several attempts.
                "PostgreSQLConnectionError",
                # Raised when merging without a primary key.
                "MissingPrimaryKeyError",
                # Raised when the database doesn't support a particular feature we use.
                # Generally, we have seen this when the database is read-only.
                "FeatureNotSupported",
                # A check constraint has been violated.
                # We do not create any ourselves, so this generally is a user-managed check, so we
                # should not retry.
                "CheckViolation",
                # We do not create foreign keys, so this is a user managed check we have failed.
                "ForeignKeyViolation",
                # Data (usually event properties) contains garbage that we cannot clean.
                "UntranslatableCharacter",
            ],
            finish_inputs=finish_inputs,
        )
