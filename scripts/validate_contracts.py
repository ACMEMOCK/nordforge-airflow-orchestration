from __future__ import annotations

import argparse
import csv
import io
import json
import zipfile
from pathlib import Path


def load_contracts(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def count_csv_rows(zf: zipfile.ZipFile, name: str) -> int:
    with zf.open(name) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        return max(sum(1 for _ in csv.reader(text)) - 1, 0)


def validate_contract_shape(contracts: dict) -> list[str]:
    issues = []
    package_ids = set()
    for package in contracts.get("packages", []):
        package_id = package.get("package_id")
        if not package_id:
            issues.append("Package is missing package_id")
            continue
        if package_id in package_ids:
            issues.append(f"Duplicate package_id: {package_id}")
        package_ids.add(package_id)

        if not package.get("expected_zip_file"):
            issues.append(f"{package_id}: missing expected_zip_file")
        if not package.get("tables"):
            issues.append(f"{package_id}: no table contracts")

        csv_names = set()
        for table in package.get("tables", []):
            if table.get("include_in_csv", True) and not table.get("csv_file"):
                issues.append(f"{package_id}/{table.get('sheet_name')}: CSV table has no csv_file")
            if table.get("csv_file"):
                if table["csv_file"] in csv_names:
                    issues.append(f"{package_id}: duplicate csv_file {table['csv_file']}")
                csv_names.add(table["csv_file"])
            if int(table.get("row_count", -1)) < 0:
                issues.append(f"{package_id}/{table.get('sheet_name')}: negative row_count")
    return issues


def validate_export_zips(contracts: dict, export_root: Path, strict_missing: bool) -> list[str]:
    issues = []
    for package in contracts.get("packages", []):
        zip_path = export_root / package["local_export_folder"] / package["expected_zip_file"]
        expected = {
            table["csv_file"]: int(table["row_count"])
            for table in package["tables"]
            if table.get("include_in_csv", True) and table.get("csv_file")
        }
        if not zip_path.exists():
            if strict_missing:
                issues.append(f"{package['package_id']}: missing {zip_path}")
            continue

        with zipfile.ZipFile(zip_path) as zf:
            actual_names = set(zf.namelist())
            if actual_names != set(expected):
                issues.append(
                    f"{package['package_id']}: zip membership mismatch "
                    f"missing={sorted(set(expected)-actual_names)} unexpected={sorted(actual_names-set(expected))}"
                )
            for name, expected_rows in expected.items():
                if name in actual_names:
                    actual_rows = count_csv_rows(zf, name)
                    if actual_rows != expected_rows:
                        issues.append(
                            f"{package['package_id']}/{name}: expected {expected_rows} rows, found {actual_rows}"
                        )
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate NordForge Airflow dataset contracts.")
    parser.add_argument("--contracts", type=Path, default=Path("include/contracts/dataset_contracts.json"))
    parser.add_argument("--export-root", type=Path, default=Path("data/exports"))
    parser.add_argument("--strict-missing", action="store_true")
    args = parser.parse_args()

    contracts = load_contracts(args.contracts)
    issues = validate_contract_shape(contracts)
    issues.extend(validate_export_zips(contracts, args.export_root, args.strict_missing))

    result = {
        "contracts": str(args.contracts),
        "export_root": str(args.export_root),
        "package_count": len(contracts.get("packages", [])),
        "table_count": sum(len(package.get("tables", [])) for package in contracts.get("packages", [])),
        "issues": issues,
    }
    print(json.dumps(result, indent=2))
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())

