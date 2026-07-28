"""Opt-in Spark integration tests against a real Docling Serve instance."""

from __future__ import annotations

import os
import site
import sys
from pathlib import Path

import pytest
from docling.datamodel.service.options import ConvertDocumentsOptions
from pyspark.sql import SparkSession

from docling_spark import DoclingConnection, chunk_documents, convert_documents

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("DOCLING_SPARK_RUN_INTEGRATION") != "1",
        reason="run with scripts/test-integration.sh",
    ),
]


def _configure_python_workers() -> None:
    """Make the active environment available to local Spark Python workers."""
    project_source = Path(__file__).resolve().parents[1] / "src"
    worker_paths = [str(project_source), *site.getsitepackages()]
    existing = os.environ.get("PYTHONPATH")
    if existing:
        worker_paths.append(existing)

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(worker_paths))
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def test_local_file_conversion_and_chunking_through_spark(tmp_path: Path) -> None:
    _configure_python_workers()
    source = tmp_path / "integration-source.md"
    source.write_text(
        "# Integration Test\n\n"
        "The Spark executor uploads this local file to Docling Serve.\n",
        encoding="utf-8",
    )

    spark = (
        SparkSession.builder.master(os.environ.get("DOCLING_SPARK_MASTER", "local[2]"))
        .appName("docling-spark-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        frame = spark.createDataFrame(
            [(str(source), "integration-document")],
            ["source_path", "document_id"],
        ).coalesce(1)
        connection = DoclingConnection(
            url=os.environ.get(
                "DOCLING_SERVE_URL",
                "http://127.0.0.1:5001",
            ),
            api_key=os.environ.get("DOCLING_SERVE_API_KEY", ""),
            max_concurrency=1,
            job_timeout=300,
        )
        options = ConvertDocumentsOptions(
            do_ocr=False,
            to_formats=["json"],
        )
        converted = convert_documents(
            frame,
            source_column="source_path",
            connection=connection,
            options=options,
        )
        chunked = chunk_documents(
            frame,
            source_column="source_path",
            connection=connection,
            options=options,
        )

        converted_row = converted.collect()[0]
        chunked_row = chunked.collect()[0]
    finally:
        spark.stop()

    assert converted_row.docling_status == "success", converted_row.docling_errors
    assert converted_row.docling_errors == "[]"
    assert converted_row.source_path == str(source)
    assert converted_row.document_id == "integration-document"
    assert converted_row.docling_document
    assert "# Integration Test" in converted_row.docling_markdown
    assert "docling_chunks" not in converted_row.asDict()

    assert chunked_row.docling_status == "success", chunked_row.docling_errors
    assert chunked_row.docling_errors == "[]"
    assert chunked_row.source_path == str(source)
    assert chunked_row.document_id == "integration-document"
    assert chunked_row.docling_chunks
    assert "Integration Test" in chunked_row.docling_chunks[0].text
    assert "docling_document" not in chunked_row.asDict()
