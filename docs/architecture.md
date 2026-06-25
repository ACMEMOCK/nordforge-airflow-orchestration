# Architecture

## Platform Intent

NordForge uses Airflow as the orchestration layer for daily ERP and operations data movement. The repository is deliberately centered on control and reliability rather than synthetic transformation volume:

- Verify generated ERP CSV packages arrived.
- Enforce dataset contracts and row counts.
- Stage orchestration manifests for downstream warehouse ingestion.
- Publish retry candidates for failed or missing interfaces.
- Expose domain-level KPI run metadata for dashboards and monitors.

## DAG Stages

1. `load_dataset_contracts`
   - Reads `include/contracts/dataset_contracts.json`.
   - Loads package, table, lineage, row-count, and ownership metadata.

2. `validate_export_packages`
   - Checks each mounted CSV zip package.
   - Confirms expected CSV membership.
   - Confirms table row counts.
   - Allows demo-mode operation if packages are not mounted.

3. `build_orchestration_plan`
   - Orders package groups in the intended industrial data flow:
     - commercial controls
     - customer success
     - warehouse execution
     - customer promise
     - order to cash
     - logistics performance
     - transport cost control

4. `publish_control_manifest`
   - Writes a JSON run manifest into `data/control`.

5. `publish_retry_queue`
   - Publishes missing or failed package checks as retry candidates.

6. `publish_kpi_manifest`
   - Summarizes contracted rows by domain and pipeline group.

## Deployment Shape

The Docker Compose stack uses:

- `postgres` for Airflow metadata
- `airflow-apiserver` for the Airflow web/API surface
- `airflow-scheduler` for scheduling
- `airflow-dag-processor` for DAG parsing
- `airflow-triggerer` for deferrable task support

The mounted data path is `data/exports`, surfaced inside the container as `/opt/airflow/data/exports`.

