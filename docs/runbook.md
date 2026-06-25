# Runbook

## Normal Daily Run

1. ERP and operations packages land under `data/exports/<package_id>/`.
2. Airflow runs `nordforge_erp_daily_orchestration` at 05:15 UTC.
3. The DAG validates every expected CSV file and row count.
4. A control manifest is written under `data/control/`.
5. Retry candidates and KPI run metadata are emitted through task outputs.

## Demo Mode

Demo mode is controlled by:

```text
NFORGE_DEMO_MODE=true
```

When demo mode is on, missing zip packages do not fail the DAG. This is useful when presenting the repository before large generated data packages are mounted.

## Strict Mode

Use strict mode when the CSV exports are expected to exist:

```text
NFORGE_DEMO_MODE=false
```

Then run:

```bash
python scripts/validate_contracts.py --strict-missing
```

## Common Failures

| Symptom | Likely Cause | Action |
| --- | --- | --- |
| Missing package | Zip not copied into `data/exports/<package_id>/` | Copy the package or restore upstream extract |
| Zip membership mismatch | Extra or missing CSV file | Regenerate the package or update the contract intentionally |
| Row-count mismatch | CSV was edited, truncated, or regenerated with a different source | Compare against the package workbook and generation summary |
| Airflow container cannot write logs | Host permissions or missing `AIRFLOW_UID` | Copy `.env.example` to `.env` and set `AIRFLOW_UID` |

## Promotion Checklist

- Replace demo credentials.
- Set a real JWT secret.
- Move secrets into a vault-backed Airflow connection strategy.
- Move CSV packages to object storage or a governed landing zone.
- Add alerting for failed DAG runs and row-count drift.
- Review contracts before adding or removing CSV files.

