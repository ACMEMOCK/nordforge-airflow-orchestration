# Roadmap

## Milestone 0: Repository Bootstrap

Status: Complete

- Airflow 3 Docker Compose starter environment
- Daily NordForge orchestration DAG
- Dataset contracts for 7 generated packages
- Local validator and standard-library tests
- CI workflow
- Runbook and architecture docs

## Milestone 1: Contract Enforcement

Goal: Make mounted generated packages fail fast when CSV membership or row counts drift.

- Add strict-mode examples for each package
- Add fixture zip packages with tiny sample CSVs
- Add contract diff reporting
- Add owner-specific failure messages

## Milestone 2: Domain TaskGroups

Goal: Split the DAG into readable domain sections that mirror the industrial data estate.

- Commercial controls
- Customer success
- Warehouse execution
- Customer promise
- Order to cash
- Logistics performance
- Transport cost control

## Milestone 3: Production Readiness

Goal: Prepare for a real Airflow deployment.

- Replace demo credentials and local secrets
- Add environment-specific configuration guidance
- Add alert routing for incidents
- Add object-storage landing-zone pattern
- Add freshness checks and SLA reporting

## Milestone 4: Observability And Lineage

Goal: Make the orchestration layer auditable across domains.

- Emit structured lineage events
- Publish manifest history
- Track package freshness and retry outcomes
- Add dashboards for contract failures and package arrival

