"""Tests for the Spark conversion boundary."""

import json
from dataclasses import dataclass

import pytest
from docling.datamodel.base_models import ConversionStatus, DocItemLabel
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.responses import (
    ChunkDocumentResponse,
    ChunkedDocumentResultItem,
)
from docling.service_client import ChunkerKind
from docling_core.types.doc import DoclingDocument
from pyspark.sql import Row

from docling_spark.dataframe import _chunk_partition, _convert_partition


@dataclass
class FakeConversionClient:
    result: ConversionResult

    def convert(
        self,
        source: str,
        *,
        options: ConvertDocumentsOptions,
        raises_on_error: bool,
    ) -> ConversionResult:
        assert source == "https://objects.example/manual.pdf"
        assert options.do_ocr is False
        assert raises_on_error is False
        return self.result


@dataclass
class FakeChunkingClient:
    result: ChunkDocumentResponse

    def chunk(
        self,
        source: str,
        chunker: ChunkerKind,
        options: ConvertDocumentsOptions | None = None,
    ) -> ChunkDocumentResponse:
        assert source == "https://objects.example/manual.pdf"
        assert chunker is ChunkerKind.HYBRID
        assert options is not None
        assert options.do_ocr is False
        return self.result


def test_convert_partition_preserves_input_and_emits_document() -> None:
    document = DoclingDocument(name="manual")
    document.add_heading(text="Installation", level=1)
    document.add_text(
        label=DocItemLabel.TEXT,
        text="Connect the appliance to power.",
    )
    result = ConversionResult.model_construct(
        status=ConversionStatus.SUCCESS,
        document=document,
        errors=[],
    )

    converted = list(
        _convert_partition(
            rows=[
                Row(
                    source="https://objects.example/manual.pdf",
                    tenant="acme",
                )
            ],
            source_column="source",
            client=FakeConversionClient(result=result),
            options=ConvertDocumentsOptions(do_ocr=False),
        )
    )

    assert len(converted) == 1
    row = converted[0]
    assert row[0:2] == ("https://objects.example/manual.pdf", "acme")
    assert row[2] == "success"
    assert '"name":"manual"' in row[4]
    assert "Installation" in row[5]


@pytest.mark.parametrize("source", ["", None, 7])
def test_convert_partition_captures_invalid_sources(source: object) -> None:
    document = DoclingDocument(name="unused")
    result = ConversionResult.model_construct(
        status=ConversionStatus.SUCCESS,
        document=document,
        errors=[],
    )

    converted = list(
        _convert_partition(
            rows=[Row(source=source)],
            source_column="source",
            client=FakeConversionClient(result=result),
            options=ConvertDocumentsOptions(),
        )
    )

    assert converted[0][1] == "failure"
    assert "non-empty string" in converted[0][2]


def test_chunk_partition_returns_server_chunks_without_document() -> None:
    result = ChunkDocumentResponse(
        chunks=[
            ChunkedDocumentResultItem(
                filename="manual.pdf",
                chunk_index=0,
                text="# Installation\n\nConnect the appliance to power.",
                raw_text="Connect the appliance to power.",
                num_tokens=12,
                headings=["Installation"],
                captions=None,
                doc_items=["#/texts/0"],
                page_numbers=[1],
                metadata={"source": "manual"},
            )
        ],
        documents=[],
        processing_time=0.1,
    )

    chunked = list(
        _chunk_partition(
            rows=[
                Row(
                    source="https://objects.example/manual.pdf",
                    tenant="acme",
                )
            ],
            source_column="source",
            client=FakeChunkingClient(result=result),
            options=ConvertDocumentsOptions(do_ocr=False),
            chunker=ChunkerKind.HYBRID,
        )
    )

    assert len(chunked) == 1
    row = chunked[0]
    assert row[0:2] == ("https://objects.example/manual.pdf", "acme")
    assert row[2:4] == ("success", "[]")
    assert row[4][0][0].startswith("# Installation")
    assert json.loads(row[4][0][1]) == {
        "filename": "manual.pdf",
        "chunk_index": 0,
        "raw_text": "Connect the appliance to power.",
        "num_tokens": 12,
        "headings": ["Installation"],
        "captions": None,
        "doc_items": ["#/texts/0"],
        "page_numbers": [1],
        "metadata": {"source": "manual"},
    }


@pytest.mark.parametrize("source", ["", None, 7])
def test_chunk_partition_captures_invalid_sources(source: object) -> None:
    result = ChunkDocumentResponse(
        chunks=[],
        documents=[],
        processing_time=0.0,
    )

    chunked = list(
        _chunk_partition(
            rows=[Row(source=source)],
            source_column="source",
            client=FakeChunkingClient(result=result),
            options=ConvertDocumentsOptions(),
            chunker=ChunkerKind.HYBRID,
        )
    )

    assert chunked[0][1] == "failure"
    assert "non-empty string" in chunked[0][2]
    assert chunked[0][3] == []
