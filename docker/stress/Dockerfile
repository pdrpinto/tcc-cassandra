# Minimal image providing cassandra-stress from Apache Cassandra tarball
FROM eclipse-temurin:17-jre

ARG CASSANDRA_VERSION=4.1.5
ENV CASSANDRA_VERSION=${CASSANDRA_VERSION}

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl ca-certificates \
	&& rm -rf /var/lib/apt/lists/* \
	&& curl -L https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz \
	   | tar -xz -C /opt \
	&& ln -s /opt/apache-cassandra-${CASSANDRA_VERSION} /opt/cassandra

ENV PATH="/opt/cassandra/bin:/opt/cassandra/tools/bin:${PATH}"

ENTRYPOINT ["cassandra-stress"]
