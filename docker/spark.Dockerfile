# Spark image: runs the Structured Streaming job and the MLlib trainer (Spark local mode).
FROM python:3.12-slim-trixie

# Must match the pyspark version (4.2.0 -> Scala 2.13, Hadoop 3.5.0).
ARG SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0"
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SPARK_PACKAGES=${SPARK_PACKAGES} \
    SPARK_IVY_DIR=/opt/ivy

# (slim images ship without man directories; the JRE's post-install step expects man1 to exist)
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /app
COPY requirements/spark.txt requirements/spark.txt
RUN pip install -r requirements/spark.txt

# Resolve the Kafka + S3A connector jars (and their deps) at build time, so containers start
# fast and do not depend on Maven Central at runtime.
RUN python -c "import os; from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', os.environ['SPARK_PACKAGES']) \
.config('spark.jars.ivy', os.environ['SPARK_IVY_DIR']).getOrCreate().stop()"

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .

CMD ["python", "-m", "shopmetric.streaming_job"]
