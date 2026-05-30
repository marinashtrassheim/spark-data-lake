FROM apache/spark:3.5.0-scala2.12-java11-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Notebook 6 + pinned jupyter-server 1.x: avoids Jupyter Server 2 /login token page.
RUN pip3 install boto3 delta-spark==3.0.0 \
    notebook==6.5.7 \
    "jupyter-server>=1.24,<2" \
    matplotlib pandas

COPY jupyter/jupyter_notebook_config.py /home/spark/.jupyter/
COPY jupyter/jupyter_server_config.py /home/spark/.jupyter/
COPY jupyter/start-notebook.sh /home/spark/start-notebook.sh

# Spark image runs as user "spark" but often has no writable $HOME — Jupyter needs it.
RUN mkdir -p /home/spark/.jupyter /home/spark/.local && \
    chmod +x /home/spark/start-notebook.sh && \
    chown -R spark:spark /home/spark

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV HOME=/home/spark

USER spark
WORKDIR /opt/spark