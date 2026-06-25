# NordForge Airflow Orchestration

Apache Airflow starter repository for NordForge Industrial Group's ERP and operations data platform.

This repository is ready to upload as a new GitHub repository. It contains a local Airflow 3 Docker Compose environment, one production-shaped orchestration DAG, dataset contracts for the generated NordForge data packages, validation scripts, tests, and runbook documentation.

## What This Orchestrates

The DAG `nordforge_erp_daily_orchestration` models the daily control plane for:

- ERP order fulfillment and ATP checks
- Customer service account health and escalation actions
- EMEA sales contract pricing and discount controls
- Logistics OTIF and delivery exception recovery
- Freight audit, accrual, rate-card, and emissions packages
- Industrial inventory movement and stock aging records
- Customer-facing availability, blocked stock, and promise queue workflows

The data contracts are based on the CSV packages generated for:

| Package | Domain | Contracted Detail Rows |
| --- | --- | ---: |
| `ACCU1932` | Order fulfillment | 10,978 |
| `ACCU7392` | Customer service accounts | 982 |
| `ACSA2026` | Sales contract pricing | 3,985 |
| `ACDA7982` | Logistics OTIF analytics | 28,946 |
| `ACSA2637` | Freight transportation | 4,795 |
| `ACDA4413` | Inventory movements | 56,489 |
| `ACCU1533` | Customer availability and ATP | 22,623 |

## Repository Layout

```text
.
|-- dags/
|   `-- nordforge_erp_daily_orchestration.py
|-- include/
|   `-- contracts/dataset_contracts.json
|-- scripts/
|   `-- validate_contracts.py
|-- tests/
|   `-- test_contracts.py
|-- docs/
|   |-- architecture.md
|   `-- runbook.md
|-- docker-compose.yaml
|-- .env.example
`-- requirements.txt
```

## Quick Start

1. Copy `.env.example` to `.env`.
2. Start the Airflow database initialization:

```bash
docker compose up airflow-init
```

3. Start Airflow:

```bash
docker compose up
```

4. Open `http://localhost:8080` and sign in with `airflow` / `airflow`.
5. Enable or trigger `nordforge_erp_daily_orchestration`.

The repository defaults to demo mode. In demo mode, the DAG validates the contracts and writes control manifests even if the generated CSV zip packages are not mounted yet.

## Validating With Real CSV Packages

Place generated CSV zip packages under:

```text
data/exports/<package_id>/<zip_file>
```

Example:

```text
data/exports/accu1533_customer_availability/ACCU1533_nordforge_customer_availability_csv_exports.zip
```

Then set this in `.env`:

```text
NFORGE_DEMO_MODE=false
```

Run:

```bash
python scripts/validate_contracts.py --strict-missing
```

## Airflow Notes

This repo follows the current Airflow Docker quick-start shape: mounted `dags`, `logs`, `config`, and `plugins` folders, plus an API server on `localhost:8080`. The Docker setup is intended for local development and demonstration, not hardened production deployment.

Official references:

- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html

