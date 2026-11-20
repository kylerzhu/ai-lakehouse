# Spark 3.5 + Paimon + PostgreSQL JDBC 驱动
# 构建上下文是 infra/（见 docker-compose.yml），jar 清单来自 infra/jars.txt。
ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root
COPY jars.txt fetch_jars.sh /tmp/
RUN sh /tmp/fetch_jars.sh spark /opt/spark/jars /tmp/jars.txt \
 && rm /tmp/jars.txt /tmp/fetch_jars.sh

COPY spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# 以 uid 9999 运行（与 Flink 镜像的 flink 用户相同），这样两边都能读写共享的 Paimon warehouse。
# 必须在 /etc/passwd 中有对应的用户名，否则 Hadoop 无法确定当前用户，读写 catalog 时会报
# "Unable to determine current user"。
RUN groupadd -g 9999 llmobs && useradd -u 9999 -g 9999 -m -d /home/llmobs -s /bin/bash llmobs
USER 9999
ENV PATH="/opt/spark/bin:${PATH}"
