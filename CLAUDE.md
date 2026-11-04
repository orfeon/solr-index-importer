# CLAUDE.md

Guidance for working on this repository. Read this before making changes.

## What this project is

`solr-index-importer` (Maven artifact; the directory may still be named `solr-plugins`) builds a Solr search
index directly from Avro files. It loads the core in-process and sends the documents through the core's update
processor chain (no HTTP), so the core's own update handler and `IndexWriter` write the index. The main use is to
build the index while building a Docker image and run that image as a read-only search node (Cloud Run).

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
| `handler` | `AvroImportHandler`, RequestHandler variant; `path`, `onInvalid`, `threads`, `dedup`, `update.chain` request params |
| `source` | `Sources` (list files / open streams for local and `gs://`), `RecordReader`, `AvroRecordReader`, `RecordReaders` (picks a reader by extension) |
| `index` | `IndexImporter` (the import loop shared by CLI and handler, invalid-record policy; writes through the update chain) |
| `convert` | `AvroToSolrDocumentConverter`, `AvroSchemas` (`unnestUnion`, `isNullable`) |
| `schema` | `SchemaGenerator` (starting-point `schema.xml` from an Avro schema), `XmlDocuments` |
| `storage` | `GcsStorage` (Cloud Storage JSON API over JDK `HttpClient`), `GoogleCredentials` (ADC-style credential resolution) |

Other files:

- `Dockerfile`: two-stage build. Stage 1 runs `importAvro` inside the official `solr` image against
  `build/conf/` and `build/data/`; stage 2 copies only the Solr home into `/var/solr/data`.
- `docker/solr.xml`: generic solr.xml baked into the image. It uses Solr 10's property names
  (`solr.security.allow.urls`, `solr.security.allow.paths`); Solr 9's `solr.allowUrls` is not read. Hybrid search
  (`CombinedQuerySearchHandler`, RRF) issues a `shards` request to the node itself even in user-managed mode, so it
  answers 403 unless the node's own URL is allowed, e.g. env `SOLR_SECURITY_ALLOW_URLS=http://localhost:8983/solr/`
  (verified with the `solr:10.1.0` image, `-e SOLR_SECURITY_ALLOW_URLS=...` and `shards=localhost:8983/solr/<core>`).
  `docker/log4j2-import.xml`: log4j2 configuration of the
  import step (importer at INFO, Solr at WARN, to stderr). Without it the importer's logs are dropped, because
  Solr's jars bring log4j2 but no configuration is on the classpath.
- `cloudbuild.yaml` + `docs/cloud-build.md`: fetch conf and Avro from GCS, build the jar, build and push the image.
- `example/`: a fictitious, self-contained sample. `conf/` is the `books` core configuration,
  `data/books.avro` holds 1,000 generated records of made-up books, `SampleData.java` regenerates that file
  (deterministic). Nothing in `example/` refers to real data, companies or projects; keep it that way.
- `build/`: staging directory for Docker builds (ignored).

## Mapping rules (behaviour that tests pin down)

- Nested records are flattened into the parent document with `parent.child` field names; arrays become
  multi-valued fields (an array of records yields one multi-valued field per leaf). The converter produces no
  Solr child documents. Consequently multi-valued `DenseVectorField`s (Solr 10.1 nested vectors) are not supported
  yet: `array<array<float>>` is flattened into one list and fails the dimension check. Since the import goes
  through the update chain, supporting them means emitting one value per inner array; Solr's
  `NestedUpdateProcessorFactory` (always run before `RunUpdateProcessor`) turns those into child documents.
- Only fields declared explicitly in the core's schema are imported (dynamic fields are not matched). The
  filter applies to leaf fields; nested records are always descended into.
- Null values are omitted, never defaulted to `""` or `0`.
- Logical `date`, `timestamp-millis`, `timestamp-micros` become `java.util.Date` (UTC); `time-*` become ISO
  local time strings; `decimal` on `bytes`/`fixed` (BigQuery NUMERIC/BIGNUMERIC) becomes `BigDecimal`
  (`generateSchema` maps it to `double`).
- Every document goes through the core's update processor chain as an `AddUpdateCommand`: the default chain, or
  the one named by `IMPORT_UPDATE_CHAIN` (CLI env), `update.chain` (handler), `UPDATE_CHAIN` (Docker),
  `_UPDATE_CHAIN` (Cloud Build). Processors configured there apply to imported documents; an unknown chain fails
  before any file is read. Each indexer thread has its own processor instance (they are not thread safe).
- The schema's `uniqueKey` is honoured (`AddUpdateCommand.overwrite`, i.e. the update handler's
  `updateDocument` on the key), so a repeated key replaces the earlier document instead of producing duplicates.
- Under the `skip` policy the whole `processAdd` call is guarded, because the schema is applied and Lucene rejects
  some documents (e.g. a single term longer than 32766 bytes) only inside the update handler.
- `IMPORT_DEDUP=false` (handler `dedup=false`, Docker `DEDUP`, Cloud Build `_DEDUP`) sets `overwrite=false`, so the
  update handler uses `addDocuments` instead of `updateDocument`; for input with unique keys the result is the same
  and the import is faster.
- The CLI replaces the whole index (delete `*:*` first), sets `IGNORE_AUTOCOMMIT` on every add so that the core's
  `autoCommit` does not commit mid-import, and logs a warning when the core has an `<updateLog>` (every document
  would also go to the tlog, which then ends up in the image). The handler adds to the existing documents, lets
  `autoCommit` work as for any update request and commits with `openSearcher=true`, so the documents are
  searchable when the request returns.
- A record that violates the schema fails the import by default. `IMPORT_ON_INVALID=skip` (CLI env),
  `onInvalid=skip` (handler), `--build-arg ON_INVALID=skip` (Docker), `_ON_INVALID=skip` (Cloud Build) skips
  and counts such records instead.

## Performance (how the import loop is built)

- `IndexImporter` is a pipeline: reader threads (one per file, at most `threads`) decode Avro into batches of 256
  records, `threads` indexer threads convert them and add them through their own processor of the update chain
  to the core's `IndexWriter`. `threads` comes from
  `IMPORT_THREADS` / handler `threads` / Docker `THREADS` / Cloud Build `_THREADS`, default
  `availableProcessors()`. `threads=1` runs the old sequential loop (deterministic order).
- Failures are recorded in an `AtomicReference` and every thread polls it; nothing blocks forever on the queue.
- `AvroImport` sets `NoMergePolicy` on the core's `IndexWriter` (a live setting) while importing, restores the
  configured policy and then commits with `optimize`, `maxOptimizeSegments=1`, `openSearcher=false` (the update
  handler runs `forceMerge(1)` and commits once). Intermediate merges would only rewrite what the final merge
  rewrites again.
- `example/conf/solrconfig.xml` sets `ramBufferSizeMB` 512; the Dockerfile runs the importer with
  `-XX:MaxRAMPercentage=70 -XX:+UseParallelGC` (`IMPORT_JAVA_OPTS`).
- Measured 2026-09-12 (500,000 sample records, 16 cores): 1 thread 25k docs/s, 16 threads 114k docs/s, plus a
  single-threaded final merge of 1.6 s / 5.3 s. `IMPORT_DEDUP=false`: 142k docs/s, merge 3.6 s. Raising the
  `ConcurrentMergeScheduler` thread counts did not shorten the final merge, so it is not configured.
- `example/SampleData.java <file> <count>` generates a larger data set for measurements
  (`build/data/books.avro 500000` was used above; `build/` is ignored).
- Measured 2026-11-04 before switching the write path from a separate `IndexWriter` (opened next to the core's,
  with `lockType none`) to the core's update processor chain (Solr 10.1.0, same 500,000 records, `example/conf`
  without `updateLog`/`autoCommit`, local Windows JVM with the Dockerfile's JVM options, 16 cores; medians of 5
  interleaved runs; a throwaway prototype). The prototype created one `UpdateRequestProcessor` per indexer thread from the default chain, sent each record as
  an `AddUpdateCommand` (`overwrite` = dedup), set `NoMergePolicy` on the core's own writer and finished with a
  `CommitUpdateCommand` (`optimize`, `maxOptimizeSegments=1`, `openSearcher=false`).

  | dedup | path | indexing | merge + commit | whole process |
  | --- | --- | --- | --- | --- |
  | true | direct `IndexWriter` | 142k docs/s | 4.9 s | 11.8 s |
  | true | update chain | 127k docs/s (−11%) | 4.8 s | 12.2 s (+3%) |
  | false | direct `IndexWriter` | 156k docs/s | 3.8 s | 10.1 s |
  | false | update chain | 144k docs/s (−8%) | 3.4 s | 10.3 s (+2%) |

  With 1 thread the gap is −2% (28.5k vs 27.9k docs/s), so the per-document cost of the chain is small; the larger
  gap at 16 threads is probably contention in `DefaultSolrCoreState.getIndexWriter` (read lock plus a
  `synchronized` block on every add), not profiled. Both paths produced 500,000 documents in one segment. Not
  measured: Docker / Cloud Build, and configurations with `updateLog` or `autoCommit`, which would slow the chain path
  further. The absolute rates are higher than the 2026-09-12 numbers above (different environment); compare only
  within one measurement.
- The switch was then made (2026-11-04). The final implementation against the old direct path, same setup, medians
  of 5 interleaved runs (the machine was slower that day than for the table above): dedup=true 120k vs 131k docs/s
  (−8%), merge + commit 6.2 vs 6.5 s, whole process 14.0 vs 13.8 s; dedup=false 135k vs 146k docs/s (−7%),
  4.4 vs 4.4 s, 11.6 vs 11.5 s. Accepted in exchange for dropping `lockType none`, the index parking on shutdown and
  the handler's private writer.

## Build and test

```shell
mvn package                  # compiles, runs the tests, produces target/solr-index-importer-0.1-full.jar
mvn -q -B clean package      # what CI-like checks should run
```

Tests are JUnit 4 (`src/test/java`), 33 as of 2026-11-04. `GcsStorageTest` uses the JDK `HttpServer` as a
fake token endpoint / storage API; no network access is needed. Keep it that way.
`IndexImporterTest` and `AvroImportTest` load real cores in-process (`provided` Solr jars are on the test
classpath): the `items` core in `src/test/resources/solr-home` (an extra `with-origin` update chain, the import
handler at `/import/avro`) and the `example/conf` `books` core with `example/data/books.avro`. Both use Solr's
default `native` lock; keep it that way so that a regression to a second writer shows up.

Running the CLI outside the Solr image needs Solr's jars and `slf4j-api` on the classpath; `generateSchema`
only needs `slf4j-api` (Avro requires it):

```shell
java -cp "target/solr-index-importer-0.1-full.jar;%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.20\slf4j-api-2.0.20.jar" \
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
java -cp "target/solr-index-importer-0.1-full.jar;%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.20\slf4j-api-2.0.20.jar" example/SampleData.java
```

## Dependency rules (learned the hard way)

- `solr-core` is `provided`. Its published POM for 10.0.0 was not resolvable by Maven (jackson deps without
  versions); 10.1.0's is. `solr-solrj`, `lucene-core` (10.4.0) and `slf4j-api` (2.0.20) stay declared
  explicitly as `provided` (we compile against them directly), with versions taken from the solr-core POM. When
  bumping Solr, take these versions from the new solr-core POM. Do not add other Solr transitives casually.
- Bundled libraries that Solr also ships (jackson 2.22.3, snappy-java 1.1.10.8, commons-compress 1.28.0 for
  Solr 10.1.0) are pinned to Solr's versions: Solr's copies come first on the classpath anyway.
- No Google client libraries. `google-cloud-storage` was removed because its stack (gax, grpc, opentelemetry,
  guava) collided with Solr's own copies inside the Solr image and made the jar 54MB. GCS access is hand-written
  over the JSON API; jackson (2.22.3, same as Solr) is the only JSON library. Do not reintroduce
  `com.google.cloud:*` or `com.google.api-client:*`.
- Bundled at runtime: avro, snappy-java (BigQuery/Dataflow write snappy Avro), commons-compress, jackson.
  Everything else must be `provided` or avoided. Check `unzip -l target/*-full.jar` after adding a dependency.
- The shade plugin uses `ManifestResourceTransformer` (main class) and `ServicesResourceTransformer`; assembly
  plugin options do not apply to shade.

## Solr 10 / Docker facts

- Solr 10 starts in SolrCloud mode by default and refuses to start without ZooKeeper. The image's `CMD` is
  `solr-foreground --user-managed`; keep it.
- In `solr:10.0.0` and `solr:10.1.0` the jars are in `/opt/solr/server/solr-webapp/webapp/WEB-INF/lib` (Solr, Lucene),
  `/opt/solr/server/lib`, `/opt/solr/server/lib/ext` (logging). The Dockerfile puts Solr's jars before the
  importer jar on the classpath (parent-first, like Solr's own plugin loading). Keep that order.
- `/var/solr` is a `VOLUME` in the base image; the index is built under `/build/solr` and `COPY`ed into
  `/var/solr/data`, because the classic builder discards `RUN` writes into a volume path.
- The importer writes only through the core's own update handler, so any `lockType` works (`example/conf` uses
  Solr's default `native`, verified with the `solr:10.1.0` image). Until 2026-11-04 it opened a second
  `IndexWriter` next to the core's, which needed `<lockType>none</lockType>`, parked the index directory during
  `CoreContainer.shutdown()` because the core's update handler could otherwise write over it, and gave the
  handler a private writer that was never closed and whose commits were not coordinated with the core's writer
  and searcher. Do not reintroduce a second writer.
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
- Hybrid search (a `shards` request from the node to itself) on Cloud Run; it was verified with the Docker image
  locally only.
