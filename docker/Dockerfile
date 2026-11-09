FROM apache/airflow:2.10.5

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Driver JDBC do SQL Server para o Spark (carga no DW). Mesma versão de dataflow_commons/spark.py.
ARG MSSQL_JDBC_VERSION=12.8.1.jre11
RUN mkdir -p /opt/jars \
    && curl -fsSL -o /opt/jars/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar \
       https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar
ENV PIPELINE_JDBC_JARS=/opt/jars/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar

USER airflow
# Fixar o apache-airflow impede que o pip o atualize ao resolver as dependências.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
