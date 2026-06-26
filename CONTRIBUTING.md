# Contributing

NordForge treats this repository as the orchestration control plane for essential ERP and operations data packages.

## Working Agreements

- Keep DAG changes small and reviewable.
- Update `include/contracts/dataset_contracts.json` whenever a package, CSV file, row count, or workbook-only tab changes.
- Run validation before opening a pull request:

```bash
python scripts/validate_contracts.py
python -m unittest discover -s tests -p "test*.py" -v
```

- Document operational impact in every PR.
- Do not commit generated CSV packages, Airflow logs, secrets, local `.env`, or database files.

## Branch Naming

Use short, readable branch names:

```text
feature/taskgroups-by-domain
fix/accu1533-contract-validation
docs/production-runbook
chore/airflow-compose-updates
```

## Review Expectations

At least one reviewer should check:

- DAG parse safety
- Contract and row-count impact
- Retry and failure behavior
- Backward compatibility of control manifests
- Documentation updates for operational changes

## Definition Of Done

- Contract validator passes.
- Tests pass.
- README or runbook is updated if behavior changed.
- Any new package/table has an owner, purpose, and row-count contract.

