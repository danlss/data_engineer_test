FROM python:3.10-bullseye

ARG AIRFLOW_VERSION=2.7.0
WORKDIR /workspace

# Install dependencies
RUN apt-get update && apt-get install -y openjdk-11-jre locales

# Configure locale
RUN sed -i -e 's/# pt_BR.UTF-8 UTF-8/pt_BR.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales

# Install Python requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Init script
COPY entrypoint.sh .
    
ENV AIRFLOW_HOME=/workspace/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
ENV AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
ENV PYTHONPATH=/workspace/src
EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
