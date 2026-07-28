"""Spark DataFrame conversion through Docling Serve."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Protocol

from docling.chunking import BaseChunker, HybridChunker
from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.service_client import DoclingServiceClient
from docling.service_client.exceptions import DoclingServiceClientError
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

OUTPUT_COLUMN_NAMES = frozenset(
    {
        "docling_status",
        "docling_errors",
        "docling_document",
        "docling_markdown",
        "docling_chunks",
    }
)


class ConversionClient(Protocol):
    """Minimal boundary implemented by ``DoclingServiceClient``."""

    def convert(
        self,
        source: str,
        *,
        options: ConvertDocumentsOptions,
        raises_on_error: bool,
    ) -> ConversionResult: ...


@dataclass(frozen=True)
class DoclingConnection:
    """Serializable settings used to create a client in each Spark partition."""

    url: str
    api_key: str = ""
    max_concurrency: int = 4
    job_timeout: float = 300.0
    http_retries: int = 3

    def create_client(self) -> DoclingServiceClient:
        """Create a client owned by the current Spark partition."""
        return DoclingServiceClient(
            url=self.url,
            api_key=self.api_key,
            max_concurrency=self.max_concurrency,
            job_timeout=self.job_timeout,
            http_retries=self.http_retries,
        )


def convert_documents(
    frame: DataFrame,
    *,
    source_column: str,
    connection: DoclingConnection,
    options: ConvertDocumentsOptions,
) -> DataFrame:
    """Convert document sources and append Delta-friendly Docling columns.

    Spark provides partition-level parallelism and retries. Each partition owns one
    reusable ``DoclingServiceClient``. Known service failures are captured on their
    source rows; unexpected programming and Spark failures still fail the task.
    """
    _validate_columns(frame=frame, source_column=source_column)
    output_schema = StructType(
        [
            *frame.schema.fields,
            StructField("docling_status", StringType(), nullable=False),
            StructField("docling_errors", StringType(), nullable=False),
            StructField("docling_document", StringType(), nullable=True),
            StructField("docling_markdown", StringType(), nullable=True),
            StructField(
                "docling_chunks",
                ArrayType(
                    StructType(
                        [
                            StructField("text", StringType(), nullable=False),
                            StructField("meta", StringType(), nullable=False),
                        ]
                    ),
                    containsNull=False,
                ),
                nullable=False,
            ),
        ]
    )
    options_json = options.model_dump_json()

    def convert_partition(rows: Iterable[Row]) -> Iterator[tuple[object, ...]]:
        with connection.create_client() as client:
            yield from _convert_partition(
                rows=rows,
                source_column=source_column,
                client=client,
                options=ConvertDocumentsOptions.model_validate_json(options_json),
                chunker=HybridChunker(),
            )

    return frame.sparkSession.createDataFrame(
        frame.rdd.mapPartitions(convert_partition),
        schema=output_schema,
    )


def _convert_partition(
    *,
    rows: Iterable[Row],
    source_column: str,
    client: ConversionClient,
    options: ConvertDocumentsOptions,
    chunker: BaseChunker,
) -> Iterator[tuple[object, ...]]:
    for row in rows:
        source = row[source_column]
        if not isinstance(source, str) or not source:
            yield (
                *tuple(row),
                ConversionStatus.FAILURE.value,
                json.dumps(["Source must be a non-empty string"]),
                None,
                None,
                [],
            )
            continue

        try:
            result = client.convert(
                source,
                options=options,
                raises_on_error=False,
            )
        except DoclingServiceClientError as error:
            yield (
                *tuple(row),
                ConversionStatus.FAILURE.value,
                json.dumps([str(error)]),
                None,
                None,
                [],
            )
            continue

        yield (*tuple(row), *_serialize_result(result=result, chunker=chunker))


def _serialize_result(
    *,
    result: ConversionResult,
    chunker: BaseChunker,
) -> tuple[str, str, str | None, str | None, list[tuple[str, str]]]:
    errors = json.dumps(
        [error.model_dump(mode="json") for error in result.errors],
        separators=(",", ":"),
    )
    if result.status not in {
        ConversionStatus.SUCCESS,
        ConversionStatus.PARTIAL_SUCCESS,
    }:
        return result.status.value, errors, None, None, []

    return (
        result.status.value,
        errors,
        json.dumps(
            result.document.export_to_dict(),
            separators=(",", ":"),
        ),
        result.document.export_to_markdown(),
        [
            (
                chunker.contextualize(chunk=chunk),
                json.dumps(
                    chunk.meta.export_json_dict(),
                    separators=(",", ":"),
                ),
            )
            for chunk in chunker.chunk(result.document)
        ],
    )


def _validate_columns(*, frame: DataFrame, source_column: str) -> None:
    if source_column not in frame.columns:
        raise ValueError(f"Source column does not exist: {source_column}")
    collisions = OUTPUT_COLUMN_NAMES.intersection(frame.columns)
    if collisions:
        raise ValueError(
            "Input DataFrame already contains Docling output columns: "
            + ", ".join(sorted(collisions))
        )
