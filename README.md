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

- Field names are the Avro field names. Nested records are flattened into the document with fields named `parent.child`. Arrays become multi-valued fields (an array of records yields one multi-valued field per leaf).
- Only fields declared explicitly in the core's schema are imported. Dynamic fields are not matched.
- Null values are omitted. A document without a value for a field has no such field in the index.
- Avro logical `date`, `timestamp-millis` and `timestamp-micros` become Solr dates (UTC). `time-millis` and `time-micros` become ISO local time strings. `decimal` (BigQuery NUMERIC/BIGNUMERIC) becomes a `BigDecimal`, which numeric and string field types accept.
- The schema's `uniqueKey` is honoured: a later record with the same key replaces the earlier one, as with Solr's update handler. With more than one indexer thread (the default) the order in which documents reach the index is not defined, so which of two duplicates survives is not defined either; use `IMPORT_THREADS=1` when that matters, or `IMPORT_DEDUP=false` to skip the replacement altogether when the input is known to have unique keys (faster).
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

Environment variables of the CLI:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SOLR_HOME` | `/var/solr/data/` | Solr home holding the core |
| `IMPORT_ON_INVALID` | `fail` | `skip` logs and counts records that violate the schema instead of failing |
| `IMPORT_THREADS` | available processors | indexer threads; `1` imports sequentially in file and record order |
| `IMPORT_DEDUP` | `true` | `false` adds documents without replacing earlier ones with the same `uniqueKey` |

The handler takes the same settings as request parameters `onInvalid`, `threads` and `dedup`.

### Performance

Reading, converting and indexing run as a pipeline: reader threads decode the Avro files into batches, indexer
threads convert the batches and add them to Lucene's `IndexWriter`, which indexes concurrently. Merges are
disabled while importing (the index is merged into one segment once at the end), and `example/conf/solrconfig.xml`
sets `ramBufferSizeMB` to 512 so that fewer segments are flushed. The Docker build runs the importer with 70%
of the container memory as heap (`--build-arg IMPORT_JAVA_OPTS=...` overrides the JVM options).

Measured with the sample schema (`text_ja` fields) on 500,000 generated records, 16 cores, Docker on Windows:

| Setting | Indexing | Final merge |
| --- | --- | --- |
| `IMPORT_THREADS=1` | 20.1 s (25k docs/s) | 1.6 s |
| 16 threads | 4.4 s (114k docs/s) | 5.3 s |
| 16 threads, `IMPORT_DEDUP=false` | 3.5 s (142k docs/s) | 3.6 s |

The final merge into one segment is single-threaded and now takes a comparable share of the time; the more
segments the threads flush, the longer it takes.

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

Build args: `CORE_NAME` (required), `SOLR_VERSION`, `ON_INVALID`, `THREADS`, `DEDUP`, `IMPORT_JAVA_OPTS`
(see the head of the `Dockerfile`). The import step logs its progress and throughput to the build output.

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
