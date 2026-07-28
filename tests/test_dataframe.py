"""Tests for the Spark conversion boundary."""

from dataclasses import dataclass

import pytest
from docling.chunking import HybridChunker
from docling.datamodel.base_models import ConversionStatus, DocItemLabel
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling_core.types.doc import DoclingDocument
from pyspark.sql import Row

from docling_spark.dataframe import _convert_partition


@dataclass
class FakeClient:
    result: ConversionResult

    def convert(
        self,
        source: str,
        *,
        options: ConvertDocumentsOptions,
        raises_on_error: bool,
    ) -> ConversionResult:
        assert source == "s3://documents/manual.pdf"
        assert options.do_ocr is False
        assert raises_on_error is False
        return self.result


def test_partition_preserves_input_and_emits_lossless_document_and_chunks() -> None:
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
            rows=[Row(source="s3://documents/manual.pdf", tenant="acme")],
            source_column="source",
            client=FakeClient(result=result),
            options=ConvertDocumentsOptions(do_ocr=False),
            chunker=HybridChunker(),
        )
    )

    assert len(converted) == 1
    row = converted[0]
    assert row[0:2] == ("s3://documents/manual.pdf", "acme")
    assert row[2] == "success"
    assert '"name":"manual"' in row[4]
    assert "Installation" in row[5]
    assert row[6]
    assert "Connect the appliance" in row[6][0][0]


@pytest.mark.parametrize("source", ["", None, 7])
def test_partition_captures_invalid_sources(source: object) -> None:
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
            client=FakeClient(result=result),
            options=ConvertDocumentsOptions(),
            chunker=HybridChunker(),
        )
    )

    assert converted[0][1] == "failure"
    assert "non-empty string" in converted[0][2]
