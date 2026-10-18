# ShopMetric — Streaming Sales Analytics and Fraud Monitoring

[![CI](https://github.com/Yaswanthmtech/shopmetric/actions/workflows/ci.yml/badge.svg)](https://github.com/Yaswanthmtech/shopmetric/actions/workflows/ci.yml)

ShopMetric is a local-first e-commerce data engineering project for exploring
stream processing, data validation, fraud signals and operational analytics.

It processes simulated checkout events through a streaming pipeline and
presents sales, fraud and pipeline information in a Streamlit dashboard.

## Features

- **Event streaming:** Kafka for checkout-event ingestion.
- **Stream processing:** Spark Structured Streaming for event processing.
- **Data validation:** Contract checks and reason-coded handling of invalid records.
- **Fraud monitoring:** Customer activity features and a machine-learning scoring pipeline.
- **Storage:** PostgreSQL and local object storage for the local demonstration.
- **Dashboard:** Streamlit views for sales, fraud alerts and pipeline health.
- **Optional integrations:** AWS S3, Snowflake and a natural-language SQL copilot,
  depending on configuration.

The default demonstration uses simulated data. Its results should not be
interpreted as evidence of performance on real customer transactions.

## Architecture

```text
Simulated checkout events
          |
          v
        Kafka
          |
          v
Spark Structured Streaming
          |
          +----> Data-quality handling
          |
          +----> Customer activity features
          |
          +----> Fraud scoring and alerts
          |
          +----> PostgreSQL / local object storage
                         |
                         v
                 Streamlit dashboard
```

## Requirements

- Docker Engine and Docker Compose
- Ubuntu on WSL 2 for Windows users
- Sufficient disk space for container images and generated data

The first startup downloads and builds the required services. Resource usage
depends on your machine and configuration.

## Run locally

Clone the repository:

```bash
git clone https://github.com/Yaswanthmtech/shopmetric.git
cd shopmetric
```

Start the application:

```bash
./shopmetric.sh up
```

Open the dashboard:

- **Dashboard:** http://localhost:8501
- **Kafka UI:** http://localhost:8080
- **Spark UI:** http://localhost:4040

The monitoring interfaces depend on the corresponding services being active.

## Demonstrate the pipeline

Inject a sample card-testing scenario:

```bash
./shopmetric.sh attack card_testing 3
```

Check service status:

```bash
./shopmetric.sh status
```

Generate a report from the current run:

```bash
./shopmetric.sh report
```

Stop the services:

```bash
./shopmetric.sh down
```

Actual output and performance measurements depend on the runtime environment
and generated workload. Use measurements from your own runs when reporting
results.

## Project structure

```text
shopmetric/
├── dashboard/       Streamlit dashboard
├── contracts/       Event contract definitions
├── docker/          Container build files
├── docs/            Setup documentation and project images
├── infra/           Infrastructure configuration
├── models/          Fraud-model artifacts
├── requirements/    Dependency specifications
├── scripts/         Utility and test scripts
├── src/shopmetric/  Application and pipeline code
├── tests/           Automated tests
├── .github/         CI workflow
├── docker-compose.yml
├── pyproject.toml
├── Makefile
└── shopmetric.sh
```

## Configuration and security

Copy `.env.example` to `.env` when local configuration is needed. Add credentials
only to your local environment files. Do not commit `.env`, `.env.laptop`,
private keys, API tokens or other secrets.

Optional cloud integrations require your own credentials and configuration.
They are not required for the default local demonstration.

## Testing

Where the required local dependencies are installed, run:

```bash
make test
make lint
```

Check the output of each command before describing the tests as passing.

## Limitations

- The default workload is simulated, not live e-commerce traffic.
- Performance measurements vary by hardware and configuration.
- Fraud scores on simulated data do not establish real-world fraud-detection
  effectiveness.
- Optional cloud services require separate setup and valid credentials.

## License

See [LICENSE](LICENSE).
