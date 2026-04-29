from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import benchmark
from src.llm_client import OpenRouterLLMClient
from src.pipeline import AnalyticsPipeline, SQLValidator
from src.types import AnswerGenerationOutput, SQLGenerationOutput


class SQLValidatorTests(unittest.TestCase):
    def test_rejects_destructive_sql(self) -> None:
        result = SQLValidator.validate("DELETE FROM gaming_mental_health", question="delete everything")
        self.assertFalse(result.is_valid)
        self.assertIn("unsafe_keyword", result.error)

    def test_rejects_multiple_statements(self) -> None:
        sql = "SELECT gender FROM gaming_mental_health; DROP TABLE gaming_mental_health"
        result = SQLValidator.validate(sql, question="show gender")
        self.assertFalse(result.is_valid)
        self.assertIn("multiple_statements", result.error)

    def test_rejects_unknown_column(self) -> None:
        sql = "SELECT zodiac_sign FROM gaming_mental_health"
        result = SQLValidator.validate(sql, question="show the unsupported metric")
        self.assertFalse(result.is_valid)
        self.assertIn("unknown_column", result.error)

    def test_rejects_sql_that_misses_expected_aggregation(self) -> None:
        sql = "SELECT anxiety_score FROM gaming_mental_health LIMIT 5"
        result = SQLValidator.validate(sql, question="What is the average anxiety score by gender?")
        self.assertFalse(result.is_valid)
        self.assertIn("missing_expected_aggregation", result.error)

    def test_allows_max_subquery_for_highest_range_question(self) -> None:
        sql = (
            "SELECT COUNT(*) FROM gaming_mental_health "
            "WHERE addiction_level = (SELECT MAX(addiction_level) FROM gaming_mental_health)"
        )
        result = SQLValidator.validate(sql, question="Roughly how many respondents fall into the highest addiction range?")
        self.assertTrue(result.is_valid)

    def test_accepts_supported_grouped_aggregation(self) -> None:
        sql = (
            "SELECT gender, AVG(anxiety_score) AS avg_anxiety "
            "FROM gaming_mental_health GROUP BY gender ORDER BY avg_anxiety DESC"
        )
        result = SQLValidator.validate(sql, question="What is the average anxiety score by gender?")
        self.assertTrue(result.is_valid)
        self.assertEqual(result.validated_sql, sql)


class TokenCountingTests(unittest.TestCase):
    def test_uses_provider_usage_when_available(self) -> None:
        client = object.__new__(OpenRouterLLMClient)
        client._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        usage = type("Usage", (), {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18})()
        response = type("Response", (), {"usage": usage})()

        client._record_response_usage(
            response,
            messages=[{"role": "user", "content": "hello"}],
            completion_text="world",
        )

        self.assertEqual(
            client._stats,
            {"llm_calls": 1, "prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18},
        )

    def test_falls_back_to_estimation_when_usage_missing(self) -> None:
        client = object.__new__(OpenRouterLLMClient)
        client._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        response = type("Response", (), {})()

        client._record_response_usage(
            response,
            messages=[{"role": "system", "content": "system"}, {"role": "user", "content": "hello world"}],
            completion_text="short answer",
        )

        self.assertEqual(client._stats["llm_calls"], 1)
        self.assertGreater(client._stats["prompt_tokens"], 0)
        self.assertGreater(client._stats["completion_tokens"], 0)
        self.assertEqual(
            client._stats["total_tokens"],
            client._stats["prompt_tokens"] + client._stats["completion_tokens"],
        )


class FollowUpTests(unittest.TestCase):
    def test_detects_follow_up_question(self) -> None:
        self.assertTrue(AnalyticsPipeline.is_follow_up_question("What about males specifically?"))

    def test_detects_fresh_question(self) -> None:
        self.assertFalse(AnalyticsPipeline.is_follow_up_question("What is the average anxiety score by gender?"))

    def test_builds_heuristic_sql_for_top_grouped_average(self) -> None:
        sql = AnalyticsPipeline.generate_heuristic_sql("What are the top 5 age groups by average addiction level?")
        self.assertIsNotNone(sql)
        self.assertIn("avg(addiction_level)", sql.lower())
        self.assertIn("group by age", sql.lower())
        self.assertIn("limit 5", sql.lower())

    def test_builds_heuristic_sql_for_gender_comparison(self) -> None:
        sql = AnalyticsPipeline.generate_heuristic_sql("How does gaming addiction level vary between genders?")
        self.assertIsNotNone(sql)
        self.assertIn("group by gender", sql.lower())
        self.assertIn("avg(addiction_level)", sql.lower())

    def test_does_not_confuse_average_with_age_dimension(self) -> None:
        sql = AnalyticsPipeline.generate_heuristic_sql("How does average anxiety score differ by addiction level?")
        self.assertIsNotNone(sql)
        self.assertIn("group by addiction_level", sql.lower())
        self.assertIn("avg(anxiety_score)", sql.lower())


class BenchmarkTests(unittest.TestCase):
    def test_benchmark_uses_pipeline_output_attributes(self) -> None:
        fake_result = type("FakeResult", (), {"status": "success", "timings": {"total_ms": 12.5}})()
        fake_pipeline = type("FakePipeline", (), {"run": lambda self, prompt: fake_result})()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            prompts_path = root / "tests"
            prompts_path.mkdir()
            (prompts_path / "public_prompts.json").write_text(json.dumps(["prompt one", "prompt two"]), encoding="utf-8")

            with patch.object(benchmark, "_ensure_gaming_db", return_value=root / "data.sqlite"), \
                 patch.object(benchmark, "AnalyticsPipeline", return_value=fake_pipeline), \
                 patch.object(benchmark, "PROJECT_ROOT", root), \
                 patch("sys.argv", ["benchmark.py", "--runs", "1"]):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    benchmark.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["success_rate"], 1.0)
        self.assertEqual(payload["samples"], 2)


class PipelineBehaviorTests(unittest.TestCase):
    def test_invalid_sql_short_circuits_to_cannot_answer(self) -> None:
        class StubLLM:
            def generate_sql(self, question: str, context: dict) -> SQLGenerationOutput:
                return SQLGenerationOutput(
                    sql="DELETE FROM gaming_mental_health",
                    timing_ms=1.0,
                    llm_stats={"llm_calls": 1, "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2, "model": "stub"},
                )

            def generate_answer(self, question: str, sql: str | None, rows: list[dict]) -> AnswerGenerationOutput:
                return AnswerGenerationOutput(
                    answer="I cannot answer this with the available table and schema. Please rephrase using known survey fields.",
                    timing_ms=0.0,
                    llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": "stub"},
                )

        pipeline = AnalyticsPipeline(llm_client=StubLLM())
        result = pipeline.run("delete the table")
        self.assertEqual(result.status, "invalid_sql")
        self.assertIn("cannot answer", result.answer.lower())


if __name__ == "__main__":
    unittest.main()
