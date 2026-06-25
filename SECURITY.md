# Security

## Supported Scope

This repository is a local/development starter for Airflow orchestration. It should not be deployed as-is to production.

## Do Not Commit

- Airflow admin passwords
- `.env`
- API tokens or cloud credentials
- Generated customer, order, freight, or inventory data
- Airflow metadata databases
- Logs that may contain operational identifiers

## Production Hardening Checklist

- Replace demo Airflow credentials.
- Replace `AIRFLOW__API_AUTH__JWT_SECRET`.
- Store secrets in a vault or managed Airflow secret backend.
- Use managed Postgres or a production metadata database.
- Add SSO or an enterprise auth manager.
- Restrict network access to Airflow services.
- Configure backup and restore for metadata and logs.
- Review package-level data classification before loading real data.

## Reporting

For this starter repository, create a private GitHub issue labeled `type: incident` and `area: security` once the repository exists in your organization.

