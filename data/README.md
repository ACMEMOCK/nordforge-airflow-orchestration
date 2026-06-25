# Local Data Mount

Keep generated ERP CSV zip packages outside Git by default.

For local Airflow validation, place package folders under:

```text
data/exports/<package_id>/<zip_file>
```

Example:

```text
data/exports/accu1533_customer_availability/ACCU1533_nordforge_customer_availability_csv_exports.zip
```

The DAG runs in demo mode unless `NFORGE_DEMO_MODE=false`.

