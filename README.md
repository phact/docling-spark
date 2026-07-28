# Docling Spark

`docling-spark` converts document-source rows with
[Docling Serve](https://github.com/docling-project/docling-serve) and returns
ordinary Spark columns suitable for Delta Lake, Unity Catalog, and downstream
Mosaic AI workflows. It targets hosted docling via docling-serve which includes both
self hosted as well as
IBM's managed offering [Docling for IBM watsonx](https://www.ibm.com/products/docling).

Docling runs remotely. Spark supplies parallelism, task retries, scheduling, and
data movement. The package depends on the service-client and conversion-core
extras from `docling-slim`; executors do not install or load Docling conversion
models or chunkers.

## Install

```bash
pip install docling-spark
```

The package must be installed on the driver and executors.

## Test

Run the unit tests without external services:

```bash
uv run pytest
```

Run the opt-in integration test against a temporary Docling Serve container:

```bash
./scripts/test-integration.sh
```

The integration runner starts the CPU image, waits for its health endpoint,
converts and chunks a temporary local Markdown file through a real `local[2]`
Spark session, and removes the container afterward. The image remains cached.
Set `DOCLING_SERVE_IMAGE` to test another image, or set `DOCLING_SERVE_URL`
(and optionally `DOCLING_SERVE_API_KEY`) to use an existing service. The same
script can be invoked from CI as an optional integration job.

## Convert a DataFrame

```python
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling_spark import DoclingConnection, convert_documents

# Keep stable object identifiers in the manifest. `presign_for_docling` is your
# platform-specific helper for producing a short-lived signed HTTPS URL.
source_df = (
    spark.table("documents.ingest_manifest")
    .select("document_id", "object_uri")
    .withColumn("_docling_source_url", presign_for_docling("object_uri"))
)

converted_df = convert_documents(
    source_df,
    source_column="_docling_source_url",
    connection=DoclingConnection(
        url=dbutils.secrets.get("docling", "url"),
        api_key=dbutils.secrets.get("docling", "api-key"),
        job_timeout=900,
    ),
    options=ConvertDocumentsOptions(
        do_ocr=True,
        force_ocr=False,
        table_mode="accurate",
        to_formats=["json"],
    ),
)

(
    converted_df
    # Input columns are preserved, so remove the bearer URL before persistence.
    .drop("_docling_source_url")
    .write.format("delta")
    .mode("append")
    .saveAsTable("documents.docling_conversions")
)
```

The original columns are preserved and these columns are appended:

| Column | Type | Meaning |
| --- | --- | --- |
| `docling_status` | string | Docling conversion status |
| `docling_errors` | JSON string | Structured conversion or service errors |
| `docling_document` | JSON string | Lossless `DoclingDocument` |
| `docling_markdown` | string | Reading-order Markdown |

The options are Docling Serve's own `ConvertDocumentsOptions`; this connector
does not maintain a second feature schema.

## Chunk a DataFrame

Use `chunk_documents` when the downstream pipeline only needs text chunks.
Docling Serve converts and chunks each source in one server-side job; Spark
executors do not run a Docling chunker.

```python
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling_spark import DoclingConnection, chunk_documents

chunked_df = chunk_documents(
    source_df,
    source_column="_docling_source_url",
    connection=DoclingConnection(
        url=dbutils.secrets.get("docling", "url"),
        api_key=dbutils.secrets.get("docling", "api-key"),
        job_timeout=900,
    ),
    options=ConvertDocumentsOptions(
        do_ocr=True,
        force_ocr=False,
        table_mode="accurate",
    ),
)

(
    chunked_df.drop("_docling_source_url")
    .write.format("delta")
    .mode("append")
    .saveAsTable("documents.docling_chunks")
)
```

The original columns are preserved and these columns are appended:

| Column | Type | Meaning |
| --- | --- | --- |
| `docling_status` | string | Docling conversion and chunking status |
| `docling_errors` | JSON string | Structured service errors |
| `docling_chunks` | array | Contextualized `text` and JSON `meta` per chunk |

This operation intentionally returns no converted document or Markdown.
Embedding and indexing the returned chunks remain downstream responsibilities.

Source values may be public or signed HTTP(S) URLs reachable by the managed
service, or local paths available on the Python executor. Native object-store
URIs such as `s3://...` must be converted to signed HTTP(S) URLs unless the
files are first materialized on each executor. Treat signed URLs as temporary
credentials: avoid logging, caching, or persisting the source column, and drop
it before writing conversion results.

## Execution semantics

One `DoclingServiceClient` is reused per Spark partition. Spark retries failed
tasks, and the service client retries transient HTTP failures. Spark task
execution is at least once: if an executor converts or chunks a document and
fails before committing its output, a task retry can submit the document again.
This does not duplicate successfully committed Delta rows, but it can repeat
paid processing.

For expensive recurring pipelines, filter already processed inputs using a
Delta table keyed by source checksum and effective Docling options before
calling `convert_documents` or `chunk_documents`.

## Security

The API key is serialized with the Spark task configuration. Retrieve it from
your platform's secret manager, restrict who can inspect job definitions and
logs, and rotate it according to your organization's policy.
