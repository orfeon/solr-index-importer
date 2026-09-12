# solr-index-importer

Builds a Solr search index directly from Avro files.
Instead of going through Solr's HTTP update path, it writes to the core's index directory with Lucene's `IndexWriter`.
The intended use is to build the index while building a Docker image, then run the image as a read-only search node (for example on Cloud Run).

## Contract

- Input: Avro files. A local file, a local directory, or `gs://bucket/prefix`.
- Output: the index directory of the specified core.
- Out of scope: connecting to data sources such as BigQuery or relational databases, extraction, and incremental loading.

Converting data sources into Avro files is the job of an upstream pipeline (BigQuery `EXPORT DATA`, Dataflow, and so on).
This tool deliberately stops at the file boundary so that the same Avro files always produce the same index.

## Layout

All code lives under `net.orfeon.solr.importer`.

| Package | Role |
| --- | --- |
| `cli` | `Command` dispatches to `AvroImport` (`importAvro`) and `GenerateSchema` (`generateSchema`) |
| `handler` | `AvroImportHandler`, the RequestHandler variant. Imports the files named by the `path` parameter |
| `source` | Input abstraction. `Sources` lists files and opens streams for local paths and GCS; `RecordReaders` picks a `RecordReader` by file extension. New formats such as Parquet are added here |
| `index` | `IndexImporter`, the import loop shared by CLI and handler; `IndexWriters` opens the writer on a core |
| `convert` | `AvroToSolrDocumentConverter`: Avro record to `SolrInputDocument` |
| `schema` | `SchemaGenerator`: a starting-point `schema.xml` derived from an Avro schema |
| `storage` | `GcsStorage`, a minimal Cloud Storage client over the JSON API with the JDK HTTP client; `GoogleCredentials` resolves credentials like Application Default Credentials |

## Mapping rules

- Field names are the Avro field names. Nested records become child documents whose fields are named `parent.child`. Arrays become multi-valued fields.
- Only fields declared explicitly in the core's schema are imported. Dynamic fields are not matched.
- Null values are omitted. A document without a value for a field has no such field in the index.
- Avro logical `date`, `timestamp-millis` and `timestamp-micros` become Solr dates (UTC). `time-millis` and `time-micros` become ISO local time strings.
- A record that violates the schema (a missing `required` field, a value the field type cannot parse) stops the import by default. Set the environment variable `IMPORT_ON_INVALID=skip` for the CLI (or pass `onInvalid=skip` to the handler) to log and skip such records instead; the skipped count is reported at the end.

## Build

```shell
mvn package
```

This produces `target/solr-index-importer-0.1-full.jar`. Only Avro, its snappy codec and jackson are bundled; `solr-core` is not (`provided` scope), and no Google client libraries are used, so the jar carries nothing that conflicts with the libraries Solr ships.

## Running the CLI

Put the jars of the Solr installation on the classpath (`/opt/solr` in the official Docker image).
The Solr home can be set with the `SOLR_HOME` environment variable (default `/var/solr/data/`). The core must already exist there with its `core.properties` and `conf/`.

```shell
java -cp "/opt/solr/lib/solr-index-importer-0.1-full.jar:/opt/solr/server/lib/*:/opt/solr/server/solr-webapp/webapp/WEB-INF/lib/*" \
  net.orfeon.solr.importer.cli.Command importAvro <coreName> <source>
```

Examples of `source`:

- `/temp/data/` : imports every `.avro` file under the directory, recursively
- `/temp/data/files.avro` : a single file
- `gs://bucket/path/to/` : every `.avro` object under the prefix (authenticated with Application Default Credentials)

### Cloud Storage access

`gs://` sources are read through the Cloud Storage JSON API. Credentials are resolved in the same order as
Application Default Credentials:

1. the service account key file named by `GOOGLE_APPLICATION_CREDENTIALS`,
2. the user credentials written by `gcloud auth application-default login`,
3. the metadata server when running on Google Cloud (GCE, Cloud Run, Cloud Build, GKE).

Not supported: workload identity federation (`external_account`) and service account impersonation.
Downloads are streamed and transient errors (HTTP 429, 5xx, connection failures) are retried three times,
but an interrupted download is not resumed. `STORAGE_EMULATOR_HOST` redirects requests to an emulator.

To get a `schema.xml` to start a new core from, derive it from the Avro schema of the data and edit it (analyzers, stored flags, vector dimensions):

```shell
java -cp "..." net.orfeon.solr.importer.cli.Command generateSchema <source> <coreName> > schema.xml
```

## Building the Docker image

The `Dockerfile` builds the index at image build time. It expects the core's `conf/` in `build/conf/` and the
Avro files in `build/data/`, and produces a plain `solr` image with the index under `/var/solr/data`.

```shell
mkdir -p build/conf build/data
cp -r example/conf/* build/conf/
cp example/data/*.avro build/data/      # or your own Avro files
mvn package
docker build --build-arg CORE_NAME=books -t solr-books .
docker run --rm -p 8983:8983 solr-books
curl "http://localhost:8983/solr/books/select?q=*:*&rows=1"
```

`example/` is a fictitious sample: `conf/` defines a `books` core and `data/books.avro` holds 1,000 generated
records of made-up books (`example/SampleData.java` regenerates it).

To build from `schema.xml` and Avro files stored in Cloud Storage with Cloud Build, see
[docs/cloud-build.md](docs/cloud-build.md) and `cloudbuild.yaml`.

## Using it as a RequestHandler

To import while Solr is running, register the handler in `solrconfig.xml`. Put the jar in `$SOLR_HOME/lib/` or in the `sharedLib` directory.

```xml
<requestHandler name="/import/avro" class="net.orfeon.solr.importer.handler.AvroImportHandler"/>
```

```shell
curl "http://localhost:8983/solr/<coreName>/import/avro?path=gs://bucket/path/to/"
```

## Related

The plugin that vectorizes query strings with an ONNX model at search time lives in the separate project `solr-onnx-query`.
