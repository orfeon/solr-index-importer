# Building the index with Cloud Build

This page describes how to turn a `schema.xml` and a set of Avro files stored in Cloud Storage into a Solr
image whose index is already built, using Cloud Build. The image is pushed to Artifact Registry and can be
run anywhere a container runs (Cloud Run, GKE, a VM).

The pipeline is defined in `cloudbuild.yaml` and the image in `Dockerfile` at the repository root.

## How it works

```
GCS                          Cloud Build                                   Artifact Registry
  conf/schema.xml   ---->   1. gcloud storage cp  ->  build/conf/
  conf/solrconfig.xml                                build/data/*.avro
  data/*.avro                2. mvn package        ->  target/solr-index-importer-0.1-full.jar
                             3. docker build       ->  stage 1: importAvro writes the index
                                                       stage 2: solr image + /var/solr/data   ---->  image:BUILD_ID
```

1. The `fetch` step copies the core's `conf/` directory and the Avro files from the two GCS locations into
   `build/` inside the build workspace.
2. The `build-jar` step builds the importer from this repository with Maven.
3. The `build-image` step runs the two-stage `Dockerfile`:
   - Stage 1 starts from the official `solr` image, lays out a Solr home under `/build/solr` (generic
     `docker/solr.xml`, `core.properties`, the fetched `conf/`) and runs
     `importAvro <core> /build/data`, which writes the index with Lucene's `IndexWriter`.
   - Stage 2 starts again from the official `solr` image and copies only `/build/solr` into `/var/solr/data`.
     The Avro files and the importer jar are not part of the final image. The image's command is
     `solr-foreground --user-managed`, because Solr 10 starts in SolrCloud mode by default and the baked
     index is a standalone core.
4. Cloud Build pushes the image tagged with the build id and `latest`.

The importer never reads GCS itself in this flow. Fetching happens in a Cloud Build step, so the Docker build
needs no credentials and the same `build/` contents always produce the same image.

## Layout in Cloud Storage

Any bucket and prefix works; the two locations are passed as substitutions.

```
gs://YOUR_BUCKET/solr/books/conf/schema.xml
gs://YOUR_BUCKET/solr/books/conf/solrconfig.xml
gs://YOUR_BUCKET/solr/books/conf/synonyms.txt          (anything solrconfig.xml / schema.xml refer to)
gs://YOUR_BUCKET/solr/books/data/part-00000.avro
gs://YOUR_BUCKET/solr/books/data/part-00001.avro
```

- `conf/` is copied as is and becomes the core's `conf/` directory. It must contain at least `schema.xml`
  and `solrconfig.xml`. A minimal `solrconfig.xml` is in `example/conf/`.
- `data/` may contain sub-directories; every `.avro` file under it is imported. Other files are ignored.
- Only fields declared explicitly in `schema.xml` are imported; see the mapping rules in the README.
  To get a starting point for `schema.xml`, run `generateSchema` against one of the Avro files.
- Avro files compressed with `snappy` or `deflate` are supported. BigQuery `EXPORT DATA ... FORMAT = 'AVRO'`
  and Dataflow's Avro sinks produce these by default.
- Records that violate `schema.xml` (for example a null in a `required` field) fail the build by default.
  Set the substitution `_ON_INVALID=skip` to skip and count them instead; the build log then shows
  `skipping invalid record ...` lines and a final `skipped N invalid records` summary.

## One-time setup

```shell
PROJECT_ID=your-project
REGION=asia-northeast1
BUCKET=YOUR_BUCKET

# Artifact Registry repository for the images
gcloud artifacts repositories create solr --repository-format=docker --location=$REGION --project=$PROJECT_ID

# The Cloud Build service account needs to read the bucket and push images.
# (For builds started with `gcloud builds submit` this is the default Cloud Build service account;
#  for triggers it is the service account configured on the trigger.)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
CB_SA=$PROJECT_NUMBER@cloudbuild.gserviceaccount.com
gcloud storage buckets add-iam-policy-binding gs://$BUCKET --member=serviceAccount:$CB_SA --role=roles/storage.objectViewer
gcloud artifacts repositories add-iam-policy-binding solr --location=$REGION --member=serviceAccount:$CB_SA --role=roles/artifactregistry.writer
```

Upload the configuration and data:

```shell
gcloud storage cp --recursive example/conf/* gs://$BUCKET/solr/books/conf/
gcloud storage cp example/data/*.avro gs://$BUCKET/solr/books/data/     # or your own Avro files
```

The `example/` directory is a fictitious sample: a `books` core and 1,000 generated records of made-up books.

## Running a build

From the repository root:

```shell
gcloud builds submit --project=$PROJECT_ID --config=cloudbuild.yaml \
  --substitutions=_CORE_NAME=books,_CONF_URI=gs://$BUCKET/solr/books/conf,_DATA_URI=gs://$BUCKET/solr/books/data,_IMAGE=$REGION-docker.pkg.dev/$PROJECT_ID/solr/books
```

The build takes a few minutes: Maven downloads the Solr dependencies, and the import time depends on the size
of the data. The build log shows the files that were fetched, one `indexed N documents from ...` line per Avro
file, and the commit.

To rebuild the index after the data in GCS changed, run the same command again. Every build produces a new
image tag, so rolling back is a matter of deploying the previous tag.

### Trigger instead of manual submit

Create a trigger on the repository with the same substitutions, so that a push (or a manual run of the trigger)
rebuilds the image:

```shell
gcloud builds triggers create github --project=$PROJECT_ID --region=$REGION \
  --name=solr-books --repo-owner=YOUR_GITHUB_ORG --repo-name=solr-index-importer --branch-pattern='^main$' \
  --build-config=cloudbuild.yaml \
  --substitutions=_CORE_NAME=books,_CONF_URI=gs://$BUCKET/solr/books/conf,_DATA_URI=gs://$BUCKET/solr/books/data,_IMAGE=$REGION-docker.pkg.dev/$PROJECT_ID/solr/books
```

A data-only refresh (no code change) can be started with `gcloud builds triggers run solr-books --branch=main`,
or from a scheduled job that runs that command after the upstream pipeline has written new Avro files.

## Deploying the image

Cloud Run, for example:

```shell
gcloud run deploy books --project=$PROJECT_ID --region=$REGION \
  --image=$REGION-docker.pkg.dev/$PROJECT_ID/solr/books:latest \
  --port=8983 --memory=2Gi --min-instances=1 --no-allow-unauthenticated
```

The index is part of the image and no volume is mounted on `/var/solr`, so every instance serves the same
index. Solr only writes logs at runtime, which land on the instance's in-memory file system.

## Reproducing the build locally

The Docker build only needs the `build/` directory, so the Cloud Build steps can be done by hand:

```shell
mkdir -p build/conf build/data
cp -r example/conf/* build/conf/
cp example/data/*.avro build/data/
mvn -q -DskipTests package
docker build --build-arg CORE_NAME=books -t solr-books .        # add --build-arg ON_INVALID=skip to tolerate bad records
docker run --rm -p 8983:8983 solr-books
curl "http://localhost:8983/solr/books/select?q=*:*&rows=1"
```

`build/` is ignored by git.

## Troubleshooting

- **`no supported input files found under: /build/data`**: the `_DATA_URI` prefix has no `.avro` files, or
  the files have another extension. Only `.avro` is picked up.
- **`invalid record in /build/data/...: [doc=...] missing required field: X`**: a record has no value for a
  field that `schema.xml` marks `required`. Either fix the data upstream, drop `required` from the field, or
  build with `_ON_INVALID=skip` (locally `--build-arg ON_INVALID=skip`) to leave such records out.
- **`Unrecognized codec: zstandard`**: the Avro files use zstd compression. Add `com.github.luben:zstd-jni`
  to `pom.xml` next to `snappy-java`, or export with snappy or deflate.
- **A field is missing from the index**: it is not declared as an explicit `<field>` in `schema.xml`.
  Dynamic fields are not matched. Nested records are flattened into `parent.child` fields, which must be
  declared the same way.
- **`LockObtainFailedException: Lock held by this virtual machine`**: `solrconfig.xml` uses Solr's default
  `native` lock. The importer opens its own `IndexWriter` next to the core's, so the core needs
  `<indexConfig><lockType>none</lockType></indexConfig>` as in `example/conf/solrconfig.xml`.
- **`core books does not exist`**: `_CORE_NAME` does not match the name used elsewhere; the core directory
  and `core.properties` are created from `_CORE_NAME`, so this only happens if the Dockerfile was edited.
- **Maven step is slow**: the Solr dependencies are downloaded on every build. To cache them, add a step that
  restores `~/.m2` from a GCS bucket before `build-jar` and saves it afterwards, or build the jar once and
  copy it from GCS instead of running Maven in the pipeline.
