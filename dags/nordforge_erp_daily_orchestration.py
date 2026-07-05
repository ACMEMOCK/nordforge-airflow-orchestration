from __future__ import annotations

import csv
import io
import json
import os
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from airflow.sdk import dag, task, task_group
except ImportError:  # Airflow 2 compatibility for local parsing tools.
    from airflow.decorators import dag, task, task_group


CONTRACT_PATH: Path = Path(
    "/opt/airflow/include/contracts/dataset_contracts.json"
)

EXPORT_ROOT: Path = Path(
    os.getenv("NFORGE_EXPORT_ROOT", "/opt/airflow/data/exports")
)

CONTROL_DIR: Path = Path(
    "/opt/airflow/data/control"
)

DEMO_MODE: bool = (
    os.getenv("NFORGE_DEMO_MODE", "true").strip().lower()
    in {"1", "true", "yes", "y"}
)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "nordforge-data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DOMAIN_STAGE_ORDER = (
    "commercial_controls",
    "customer_success",
    "warehouse_execution",
    "customer_promise",
    "order_to_cash",
    "logistics_performance",
    "transport_cost_control",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _csv_row_count(
    zf: zipfile.ZipFile,
    name: str,
) -> int:
    with zf.open(name) as raw:
        text = io.TextIOWrapper(
            raw,
            encoding="utf-8-sig",
            newline="",
        )
        return max(sum(1 for _ in csv.reader(text)) - 1, 0)


@dag(
    dag_id="nordforge_erp_daily_orchestration",
    description=(
        "Daily orchestration for NordForge ERP extracts, inventory, logistics, "
        "freight, and customer-service data products."
    ),
    schedule="15 5 * * *",
    start_date=datetime(2026, 6, 24),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[
        "nordforge",
        "erp",
        "industrial",
        "essential",
        "data-platform",
    ],
)
def nordforge_erp_daily_orchestration():
    @task
    def load_dataset_contracts() -> dict[str, Any]:
        contracts = json.loads(
            CONTRACT_PATH.read_text(encoding="utf-8")
        )

        return {
            "contract_version": contracts["contract_version"],
            "company": contracts["company"],
            "required_packages": contracts["required_packages"],
            "packages": contracts["packages"],
            "lineage_edges": contracts["lineage_edges"],
            "loaded_at_utc": _utc_now_iso(),
        }

    @task
    def validate_export_packages(
        contracts: dict[str, Any],
    ) -> dict[str, Any]:
        package_results = []
        issues = []

        for package in contracts["packages"]:
            package_dir = (
                EXPORT_ROOT / package["local_export_folder"]
            )
            zip_path = (
                package_dir / package["expected_zip_file"]
            )

            expected_csvs = {
                table["csv_file"]: table["row_count"]
                for table in package["tables"]
                if table.get("include_in_csv", True)
                and table.get("csv_file")
            }

            if not zip_path.exists():
                result = {
                    "package_id": package["package_id"],
                    "domain": package["domain"],
                    "pipeline_group": package["pipeline_group"],
                    "status": (
                        "not_mounted_demo"
                        if DEMO_MODE
                        else "missing"
                    ),
                    "zip_path": str(zip_path),
                    "expected_csv_files": len(expected_csvs),
                    "validated_csv_files": 0,
                    "expected_detail_rows": package[
                        "detail_rows_preserved"
                    ],
                    "validated_detail_rows": None,
                    "issues": (
                        []
                        if DEMO_MODE
                        else [f"Missing zip package: {zip_path}"]
                    ),
                }

                package_results.append(result)

                if not DEMO_MODE:
                    issues.extend(result["issues"])

                continue

            with zipfile.ZipFile(zip_path) as zf:
                actual_names = set(zf.namelist())

                missing = sorted(
                    set(expected_csvs) - actual_names
                )

                unexpected = sorted(
                    actual_names - set(expected_csvs)
                )

                row_mismatches = []

                for csv_name, expected_rows in expected_csvs.items():
                    if csv_name not in actual_names:
                        continue

                    actual_rows = _csv_row_count(
                        zf,
                        csv_name,
                    )

                    if actual_rows != expected_rows:
                        row_mismatches.append(
                            {
                                "csv_file": csv_name,
                                "expected_rows": expected_rows,
                                "actual_rows": actual_rows,
                            }
                        )

            result_issues = []

            if missing:
                result_issues.append(
                    f"Missing CSV files: {missing}"
                )

            if unexpected:
                result_issues.append(
                    f"Unexpected CSV files: {unexpected}"
                )

            if row_mismatches:
                result_issues.append(
                    f"Row count mismatches: {row_mismatches}"
                )

            primary_table = next(
                (
                    table
                    for table in package["tables"]
                    if table.get("include_in_csv", True)
                    and table.get("csv_file")
                ),
                {},
            )

            validated_detail_rows = None

            if (
                primary_table.get("csv_file")
                and primary_table["csv_file"] in expected_csvs
            ):
                validated_detail_rows = expected_csvs[
                    primary_table["csv_file"]
                ]

            package_results.append(
                {
                    "package_id": package["package_id"],
                    "domain": package["domain"],
                    "pipeline_group": package[
                        "pipeline_group"
                    ],
                    "status": (
                        "valid"
                        if not result_issues
                        else "invalid"
                    ),
                    "zip_path": str(zip_path),
                    "expected_csv_files": len(expected_csvs),
                    "validated_csv_files": len(actual_names),
                    "expected_detail_rows": package[
                        "detail_rows_preserved"
                    ],
                    "validated_detail_rows": (
                        validated_detail_rows
                    ),
                    "issues": result_issues,
                }
            )

            issues.extend(result_issues)

        if issues:
            raise ValueError(
                json.dumps(
                    {"validation_issues": issues},
                    indent=2,
                )
            )

        return {
            "validated_at_utc": _utc_now_iso(),
            "demo_mode": DEMO_MODE,
            "export_root": str(EXPORT_ROOT),
            "package_results": package_results,
        }

    @task
    def build_orchestration_plan(
        contracts: dict[str, Any],
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        packages_by_group = {
            group: [
                package["package_id"]
                for package in contracts["packages"]
                if package["pipeline_group"] == group
            ]
            for group in DOMAIN_STAGE_ORDER
        }

        return {
            "planned_at_utc": _utc_now_iso(),
            "stage_order": list(DOMAIN_STAGE_ORDER),
            "packages_by_group": packages_by_group,
        }

    @task
    def publish_control_manifest(
        contracts: dict[str, Any],
        validation: dict[str, Any],
        plan: dict[str, Any],
    ) -> dict[str, Any]:
        CONTROL_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        run_id = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )

        manifest_path = (
            CONTROL_DIR
            / f"nordforge_orchestration_manifest_{run_id}.json"
        )

        manifest = {
            "run_id": run_id,
            "company": contracts["company"],
            "contract_version": contracts["contract_version"],
            "created_at_utc": _utc_now_iso(),
            "demo_mode": validation["demo_mode"],
            "export_root": validation["export_root"],
            "package_results": validation["package_results"],
            "orchestration_plan": plan,
        }

        manifest_path.write_text(
            json.dumps(
                manifest,
                indent=2,
            ),
            encoding="utf-8",
        )

        return {
            "manifest_path": str(manifest_path),
            "run_id": run_id,
        }

    @task
    def publish_retry_queue(
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        retry_candidates = []

        for result in validation["package_results"]:
            if result["status"] != "valid":
                retry_candidates.append(
                    {
                        "package_id": result["package_id"],
                        "pipeline_group": result[
                            "pipeline_group"
                        ],
                        "status": result["status"],
                        "action": (
                            "monitor_demo_mount"
                            if result["status"]
                            == "not_mounted_demo"
                            else "retry_source_interface"
                        ),
                        "owner_hint": (
                            "NordForge Data Platform"
                        ),
                    }
                )

        return {
            "retry_candidates": retry_candidates,
            "retry_candidate_count": len(
                retry_candidates
            ),
            "published_at_utc": _utc_now_iso(),
        }

    @task
    def publish_kpi_manifest(
        contracts: dict[str, Any],
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        totals_by_group: dict[str, int] = {}
        rows_by_domain: dict[str, int] = {}

        for package in contracts["packages"]:
            pipeline_group = package["pipeline_group"]
            detail_rows = package["detail_rows_preserved"]

            totals_by_group[pipeline_group] = (
                totals_by_group.get(pipeline_group, 0)
                + detail_rows
            )

            rows_by_domain[
                package["domain"]
            ] = detail_rows

        valid_packages = sum(
            1
            for result in validation["package_results"]
            if result["status"] == "valid"
        )

        return {
            "published_at_utc": _utc_now_iso(),
            "package_count": len(contracts["packages"]),
            "valid_package_count": valid_packages,
            "contracted_detail_rows": sum(
                rows_by_domain.values()
            ),
            "rows_by_domain": rows_by_domain,
            "rows_by_pipeline_group": totals_by_group,
        }

    @task
    def evaluate_domain_stage(
        pipeline_group: str,
        contracts: dict[str, Any],
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        package_ids = [
            package["package_id"]
            for package in contracts["packages"]
            if package["pipeline_group"] == pipeline_group
        ]

        validation_by_package = {
            result["package_id"]: result["status"]
            for result in validation["package_results"]
        }

        package_statuses = {
            package_id: validation_by_package.get(
                package_id,
                "not_reported",
            )
            for package_id in package_ids
        }

        accepted_statuses = {
            "valid",
            "not_mounted_demo",
        }

        return {
            "pipeline_group": pipeline_group,
            "package_ids": package_ids,
            "package_statuses": package_statuses,
            "ready": (
                bool(package_ids)
                and all(
                    status in accepted_statuses
                    for status in package_statuses.values()
                )
            ),
            "evaluated_at_utc": _utc_now_iso(),
        }

    def build_domain_task_group(
        group_id: str,
        contracts: Any,
        validation: Any,
    ) -> Any:
        @task_group(group_id=group_id)
        def domain_task_group() -> Any:
            return evaluate_domain_stage.override(
                task_id="evaluate_readiness"
            )(
                pipeline_group=group_id,
                contracts=contracts,
                validation=validation,
            )

        return domain_task_group()

    contracts = load_dataset_contracts()

    validation = validate_export_packages(
        contracts
    )

    plan = build_orchestration_plan(
        contracts,
        validation,
    )

    domain_stages = [
        build_domain_task_group(
            group_id,
            contracts,
            validation,
        )
        for group_id in DOMAIN_STAGE_ORDER
    ]

    for previous_stage, next_stage in zip(
        domain_stages,
        domain_stages[1:],
    ):
        previous_stage >> next_stage

    manifest = publish_control_manifest(
        contracts,
        validation,
        plan,
    )

    retry_queue = publish_retry_queue(
        validation
    )

    kpi_manifest = publish_kpi_manifest(
        contracts,
        validation,
    )

    plan >> domain_stages[0]
    domain_stages[-1] >> manifest

    manifest >> retry_queue
    manifest >> kpi_manifest


nordforge_erp_daily_orchestration()
