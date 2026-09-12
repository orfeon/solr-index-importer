# Builds a Solr image with the index of one core baked in.
#
# Expected build context (see docs/cloud-build.md for how it is assembled):
#   target/solr-index-importer-0.1-full.jar   the importer, produced by `mvn package`
#   docker/solr.xml                            generic solr.xml
#   build/conf/                                the core's conf directory (schema.xml, solrconfig.xml, ...)
#   build/data/                                Avro files to import (searched recursively)
#
# Build args:
#   CORE_NAME     name of the core (required)
#   SOLR_VERSION  tag of the official solr image (default 10.0.0)
#   ON_INVALID    fail (default) or skip: what to do with records that violate the schema
#   THREADS       indexer threads (default: the processors available to the build container)
#   DEDUP         true (default) or false: false adds documents without replacing earlier ones with the same
#                 uniqueKey; faster, for input known to have unique keys
#   IMPORT_JAVA_OPTS  JVM options of the import step (default: 70% of the container memory as heap, parallel GC)
ARG SOLR_VERSION=10.0.0

# ---- stage 1: build the index ------------------------------------------------
FROM solr:${SOLR_VERSION} AS importer
ARG CORE_NAME
ARG ON_INVALID=fail
ARG THREADS=
ARG DEDUP=true
ARG IMPORT_JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+UseParallelGC"
ARG IMPORTER_JAR=target/solr-index-importer-0.1-full.jar

# The index is built under /build, outside /var/solr. The base image declares /var/solr as a VOLUME and
# the classic builder discards files that a RUN step writes into a volume path.
USER root
RUN mkdir -p /build/solr /build/data /build/lib && chown -R solr:solr /build
USER solr

COPY --chown=solr:solr ${IMPORTER_JAR} /build/lib/
COPY --chown=solr:solr docker/solr.xml /build/solr/
COPY --chown=solr:solr docker/log4j2-import.xml /build/lib/
COPY --chown=solr:solr build/conf/ /build/solr/${CORE_NAME}/conf/
COPY --chown=solr:solr build/data/ /build/data/
RUN echo "name=${CORE_NAME}" > /build/solr/${CORE_NAME}/core.properties

# solr-core is not bundled in the importer jar, so Solr's own jars are put on the classpath.
# Solr's jars come first so that Solr's classes always see the library versions they were built against
# (the importer jar bundles jackson and snappy, which Solr ships too). This mirrors how Solr loads plugin jars
# from its lib directory (parent-first).
# The wildcards cover both the webapp layout and the flat server/lib layout; missing directories are ignored.
RUN SOLR_HOME=/build/solr IMPORT_ON_INVALID=${ON_INVALID} IMPORT_THREADS=${THREADS} IMPORT_DEDUP=${DEDUP}     java ${IMPORT_JAVA_OPTS} -Dlog4j.configurationFile=/build/lib/log4j2-import.xml -cp "/opt/solr/server/solr-webapp/webapp/WEB-INF/lib/*:/opt/solr/server/lib/*:/opt/solr/server/lib/ext/*:/build/lib/*" \
    net.orfeon.solr.importer.cli.Command importAvro ${CORE_NAME} /build/data

# ---- stage 2: runtime image --------------------------------------------------
# Only the Solr home (solr.xml, the core's conf and its index) is carried over; the Avro files and the
# importer jar stay behind. COPY into a volume path is preserved, unlike RUN.
FROM solr:${SOLR_VERSION}
COPY --from=importer --chown=solr:solr /build/solr /var/solr/data
ENV SOLR_PORT=8983
# Solr 10 starts in SolrCloud mode by default and refuses to start without ZooKeeper.
# The baked index is a single user-managed (standalone) core.
CMD ["solr-foreground", "--user-managed"]
