# CLAUDE.md

Guidance for working on this repository. Read this before making changes.

## What this project is

`solr-index-importer` (Maven artifact; the directory may still be named `solr-plugins`) builds a Solr search
index directly from Avro files. It writes to the core's index directory with Lucene's `IndexWriter` instead of
going through Solr's HTTP update path. The main use is to build the index while building a Docker image and run
that image as a read-only search node (Cloud Run).

### Contract (do not widen it)

- Input: Avro files at a local path (file or directory) or `gs://bucket/prefix`.
- Output: the index directory of one core.
- Out of scope: connecting to BigQuery, relational databases or any other data source; extraction; incremental
  loading. Those belong to upstream pipelines (BigQuery `EXPORT DATA`, Dataflow). Do not propose connectors.
- New input formats (e.g. Parquet) go into `source/` behind `RecordReaders`. Add Parquet only when actually
  needed, because `parquet-avro` drags in Hadoop.

## Layout

Everything lives under `net.orfeon.solr.importer`. There is deliberately no `util` package.

| Package | Contents |
| --- | --- |
| `cli` | `Command` (dispatcher, jar main class), `AvroImport` (`importAvro <core> <source>`), `GenerateSchema` (`generateSchema <source> [core]`) |
| `handler` | `AvroImportHandler`, RequestHandler variant; `path` and `onInvalid` request params |
| `source` | `Sources` (list files / open streams for local and `gs://`), `RecordReader`, `AvroRecordReader`, `RecordReaders` (picks a reader by extension) |
| `index` | `IndexImporter` (the import loop shared by CLI and handler, invalid-record policy), `IndexWriters` |
| `convert` | `AvroToSolrDocumentConverter`, `AvroSchemas` (`unnestUnion`, `isNullable`) |
| `schema` | `SchemaGenerator` (starting-point `schema.xml` from an Avro schema), `XmlDocuments` |
| `storage` | `GcsStorage` (Cloud Storage JSON API over JDK `HttpClient`), `GoogleCredentials` (ADC-style credential resolution) |

Other files:

- `Dockerfile`: two-stage build. Stage 1 runs `importAvro` inside the official `solr` image against
  `build/conf/` and `build/data/`; stage 2 copies only the Solr home into `/var/solr/data`.
- `docker/solr.xml`: generic solr.xml baked into the image.
- `cloudbuild.yaml` + `docs/cloud-build.md`: fetch conf and Avro from GCS, build the jar, build and push the image.
- `example/`: a fictitious, self-contained sample. `conf/` is the `books` core configuration,
  `data/books.avro` holds 1,000 generated records of made-up books, `SampleData.java` regenerates that file
  (deterministic). Nothing in `example/` refers to real data, companies or projects; keep it that way.
- `build/`: staging directory for Docker builds (ignored).

## Mapping rules (behaviour that tests pin down)

- Nested records are flattened into the parent document with `parent.child` field names; arrays become
  multi-valued fields (an array of records yields one multi-valued field per leaf). Solr child documents are
  not used: the index is written with a plain `IndexWriter`, and `DocumentBuilder.toDocument(doc, schema)`
  silently drops child documents.
- Only fields declared explicitly in the core's schema are imported (dynamic fields are not matched). The
  filter applies to leaf fields; nested records are always descended into.
- Null values are omitted, never defaulted to `""` or `0`.
- Logical `date`, `timestamp-millis`, `timestamp-micros` become `java.util.Date` (UTC); `time-*` become ISO
  local time strings; `decimal` on `bytes`/`fixed` (BigQuery NUMERIC/BIGNUMERIC) becomes `BigDecimal`
  (`generateSchema` maps it to `double`).
- The schema's `uniqueKey` is honoured (`IndexWriter.updateDocument` on the key term), so a repeated key
  replaces the earlier document instead of producing duplicates.
- Under the `skip` policy the Lucene `addDocument`/`updateDocument` call is inside the guarded block too,
  because Lucene rejects some documents only there (e.g. a single term longer than 32766 bytes).
- A record that violates the schema fails the import by default. `IMPORT_ON_INVALID=skip` (CLI env),
  `onInvalid=skip` (handler), `--build-arg ON_INVALID=skip` (Docker), `_ON_INVALID=skip` (Cloud Build) skips
  and counts such records instead.

## Build and test

```shell
mvn package                  # compiles, runs the tests, produces target/solr-index-importer-0.1-full.jar
mvn -q -B clean package      # what CI-like checks should run
```

Tests are JUnit 4 (`src/test/java`), 21 as of 2026-09-12. `GcsStorageTest` uses the JDK `HttpServer` as a
fake token endpoint / storage API; no network access is needed. Keep it that way.

Running the CLI outside the Solr image needs Solr's jars and `slf4j-api` on the classpath; `generateSchema`
only needs `slf4j-api` (Avro requires it):

```shell
java -cp "target/solr-index-importer-0.1-full.jar;%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.17\slf4j-api-2.0.17.jar" \
  net.orfeon.solr.importer.cli.Command generateSchema example/data/books.avro books
```

End-to-end check with Docker (verified to work):

```shell
mkdir -p build/conf build/data && cp -r example/conf/* build/conf/ && cp example/data/*.avro build/data/
mvn -q -DskipTests package
docker build --build-arg CORE_NAME=books -t solr-books .
docker run --rm -p 8983:8983 solr-books
curl "http://localhost:8983/solr/books/select?q=*:*&rows=0"     # expect numFound 1000 with the sample data
```

To regenerate the sample data after changing `example/SampleData.java`:

```shell
java -cp "target/solr-index-importer-0.1-full.jar;%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.17\slf4j-api-2.0.17.jar" example/SampleData.java
```

## Dependency rules (learned the hard way)

- `solr-core` is `provided`. Its published POM for 10.0.0 is not resolvable by Maven (jackson deps without
  versions), so `solr-solrj`, `lucene-core` (10.3.2) and `slf4j-api` (2.0.17) are declared explicitly as
  `provided`, with versions taken from the solr-core POM. Do not add other Solr transitives casually.
- No Google client libraries. `google-cloud-storage` was removed because its stack (gax, grpc, opentelemetry,
  guava) collided with Solr's own copies inside the Solr image and made the jar 54MB. GCS access is hand-written
  over the JSON API; jackson (2.20.0, same as Avro and Solr) is the only JSON library. Do not reintroduce
  `com.google.cloud:*` or `com.google.api-client:*`.
- Bundled at runtime: avro, snappy-java (BigQuery/Dataflow write snappy Avro), commons-compress, jackson.
  Everything else must be `provided` or avoided. Check `unzip -l target/*-full.jar` after adding a dependency.
- The shade plugin uses `ManifestResourceTransformer` (main class) and `ServicesResourceTransformer`; assembly
  plugin options do not apply to shade.

## Solr 10 / Docker facts

- Solr 10 starts in SolrCloud mode by default and refuses to start without ZooKeeper. The image's `CMD` is
  `solr-foreground --user-managed`; keep it.
- In `solr:10.0.0` the jars are in `/opt/solr/server/solr-webapp/webapp/WEB-INF/lib` (Solr, Lucene),
  `/opt/solr/server/lib`, `/opt/solr/server/lib/ext` (logging). The Dockerfile puts Solr's jars before the
  importer jar on the classpath (parent-first, like Solr's own plugin loading). Keep that order.
- `/var/solr` is a `VOLUME` in the base image; the index is built under `/build/solr` and `COPY`ed into
  `/var/solr/data`, because the classic builder discards `RUN` writes into a volume path.
- The importer opens a second `IndexWriter` next to the one the loaded core already holds (the core's searcher
  is an NRT reader over the core's own writer). This only works with `<lockType>none</lockType>` in
  `solrconfig.xml` (as in `example/conf`); Solr's default `native` lock makes `IndexWriters.create` fail with
  `LockObtainFailedException`. The proper fix is to write through the core's own writer / update handler,
  which would also make `shutdownPreservingIndex` and the handler's private writer unnecessary.
- `AvroImport` parks the index directory during `CoreContainer.shutdown()` and moves it back afterwards
  (`shutdownPreservingIndex`); the core's own update handler can otherwise write into the index on shutdown.
- The sample data is clean (every record satisfies `example/conf/schema.xml`), so the end-to-end check runs
  with the default `ON_INVALID=fail`. About a tenth of the records have a null `description`, which exercises
  the null-omission rule.

## Conventions

- All documentation, comments and log messages are in English.
- No `util` grab-bag classes; put code in the package that names its role.
- Tests accompany behaviour changes in `convert`, `schema`, `source` and `storage`.
- Do not commit unless asked. The user runs git themselves.
- When something cannot be verified locally (Cloud Build execution, metadata-server credentials on GCP), say so
  explicitly in the final report instead of implying it works.

## Not verified yet

- Running `cloudbuild.yaml` on Cloud Build (the fetch step with `gcloud storage cp`, the Maven step).
- `GoogleCredentials` against a real metadata server; the file-based flows were verified against a real bucket
  with `gcloud` user credentials.
