from __future__ import annotations

import ast
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "include" / "contracts" / "dataset_contracts.json"
DAG_PATH = ROOT / "dags" / "nordforge_erp_daily_orchestration.py"


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contracts = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    def test_contract_package_and_table_counts_are_expected(self) -> None:
        self.assertEqual(self.contracts["required_packages"], 7)
        self.assertEqual(len(self.contracts["packages"]), 7)
        self.assertEqual(sum(len(package["tables"]) for package in self.contracts["packages"]), 112)

    def test_total_detail_rows_match_generated_nordforge_packages(self) -> None:
        total_rows = sum(package["detail_rows_preserved"] for package in self.contracts["packages"])
        self.assertEqual(total_rows, 128_798)

    def test_package_ids_and_zip_names_are_unique(self) -> None:
        package_ids = [package["package_id"] for package in self.contracts["packages"]]
        zip_names = [package["expected_zip_file"] for package in self.contracts["packages"]]
        self.assertEqual(len(package_ids), len(set(package_ids)))
        self.assertEqual(len(zip_names), len(set(zip_names)))

    def test_accu1533_reference_tabs_are_workbook_only(self) -> None:
        package = next(
            p for p in self.contracts["packages"] if p["package_id"] == "accu1533_customer_availability"
        )
        workbook_only = {
            table["sheet_name"]
            for table in package["tables"]
            if not table.get("include_in_csv", True)
        }
        self.assertEqual(workbook_only, {"Dataset_Links", "CSV_Reference"})
        for table in package["tables"]:
            if table.get("include_in_csv", True):
                csv_file = table.get("csv_file", "").lower()
                self.assertNotIn("dataset_links", csv_file)
                self.assertNotIn("csv_reference", csv_file)

    def test_dag_file_is_valid_python(self) -> None:
        ast.parse(DAG_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

