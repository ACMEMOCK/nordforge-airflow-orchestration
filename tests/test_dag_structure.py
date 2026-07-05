from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "dags" / "nordforge_erp_daily_orchestration.py"

EXPECTED_GROUPS = (
    "commercial_controls",
    "customer_success",
    "warehouse_execution",
    "customer_promise",
    "order_to_cash",
    "logistics_performance",
    "transport_cost_control",
)


class DagStructureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = DAG_PATH.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_domain_stage_order_matches_operating_model(self) -> None:
        assignments = {
            target.id: ast.literal_eval(node.value)
            for node in self.tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        self.assertEqual(assignments["DOMAIN_STAGE_ORDER"], EXPECTED_GROUPS)

    def test_existing_task_functions_are_preserved(self) -> None:
        function_names = {
            node.name for node in ast.walk(self.tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        expected = {
            "load_dataset_contracts",
            "validate_export_packages",
            "build_orchestration_plan",
            "publish_control_manifest",
            "publish_retry_queue",
            "publish_kpi_manifest",
        }
        self.assertTrue(expected.issubset(function_names))

    def test_task_group_factory_and_readiness_task_exist(self) -> None:
        self.assertIn("@task_group(group_id=group_id)", self.source)
        self.assertIn('task_id="evaluate_readiness"', self.source)


if __name__ == "__main__":
    unittest.main()
