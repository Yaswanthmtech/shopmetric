# App image: Kafka producer, bootstrap job, Streamlit dashboard, report CLI (no JVM needed).
FROM python:3.12-slim-trixie

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements/app.txt requirements/app.txt
RUN pip install -r requirements/app.txt

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .

COPY dashboard ./dashboard
COPY .streamlit ./.streamlit

EXPOSE 8501
CMD ["python", "-m", "shopmetric.producer"]
