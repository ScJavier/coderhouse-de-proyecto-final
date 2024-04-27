FROM apache/airflow:2.9.0

USER root

RUN apt-get update \
    && apt-get install -y \
        gcc \
        python3.9-dev \
        libpq-dev

USER airflow

# Preparing environment
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir  --progress-bar off -r requirements.txt

ENV PATH="/opt/airflow/src:/opt/airflow/dags:$[PATH}"
ENV PYTHONPATH="/opt/airflow:/opt/airflow/dags:$[PYTHONPATH]"

# Airflow default entrypoint
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []