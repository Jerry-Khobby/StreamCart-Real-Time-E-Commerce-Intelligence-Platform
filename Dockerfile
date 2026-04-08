FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
  openjdk-17-jre-headless \
  curl \
  git \
  ca-certificates \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install Docker client from official Docker repository (latest version)
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/ | \
  grep -oP 'docker-\d+\.\d+\.\d+\.tgz' | sort -V | tail -n1 | \
  xargs -I {} curl -fsSL https://download.docker.com/linux/static/stable/x86_64/{} -o docker.tgz && \
  tar -xzf docker.tgz --strip-components=1 -C /usr/local/bin docker/docker && \
  rm docker.tgz

# Install docker-compose
RUN curl -SL "https://github.com/docker/compose/releases/download/v2.27.1/docker-compose-linux-x86_64" \
  -o /usr/local/bin/docker-compose && \
  chmod +x /usr/local/bin/docker-compose

WORKDIR /opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

USER airflow

COPY --chown=airflow:root jars/postgresql-42.7.6.jar /opt/airflow/jars/
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir --use-deprecated=legacy-resolver -r requirements.txt