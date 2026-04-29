from __future__ import annotations

import re
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.gaming_csv_to_db import DEFAULT_CSV_PATH, csv_to_sqlite
from src.llm_client import OpenRouterLLMClient, build_default_llm_client
from src.observability import log_event
from src.schema import SCHEMA_COLUMNS, SUPPORTED_DIMENSIONS, TABLE_NAME
from src.types import (
    AnswerGenerationOutput,
    PipelineOutput,
    SQLExecutionOutput,
    SQLGenerationOutput,
    SQLValidationOutput,
)


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = BASE_DIR / "data" / "gaming_mental_health.sqlite"
SQLITE_FUNCTIONS = {
    "avg",
    "count",
    "min",
    "max",
    "sum",
    "round",
    "cast",
    "coalesce",
    "case",
    "when",
    "then",
    "else",
    "end",
    "distinct",
    "asc",
    "desc",
    "as",
    "and",
    "or",
    "not",
    "null",
    "is",
    "in",
    "like",
    "between",
    "limit",
    "offset",
    "having",
    "order",
    "group",
    "by",
    "from",
    "where",
    "on",
    "select",
    "with",
}
UNSAFE_SQL_KEYWORDS = ("insert", "update", "delete", "drop", "alter", "attach", "pragma", "create", "replace")
UNSUPPORTED_QUESTION_KEYWORDS = ("zodiac", "horoscope", "astrology", "star sign")
FOLLOW_UP_PATTERNS = (
    "what about",
    "how about",
    "specifically",
    "those",
    "them",
    "that group",
    "same",
    "instead",
    "now sort",
    "for males",
    "for females",
)
MEASURE_ALIASES = {
    "addiction level": "addiction_level",
    "anxiety score": "anxiety_score",
    "stress score": "stress_level",
    "stress level": "stress_level",
    "depression score": "depression_score",
}
DIMENSION_ALIASES = {
    "gender": "gender",
    "age group": "age",
    "age groups": "age",
    "age": "age",
    "addiction level": "addiction_level",
    "addiction levels": "addiction_level",
}


class SQLValidationError(Exception):
    pass


@dataclass
class TurnContext:
    question: str
    sql: str | None
    answer: str
    rows: list[dict[str, Any]] = field(default_factory=list)


class SQLValidator:
    @classmethod
    def validate(cls, sql: str | None, question: str | None = None) -> SQLValidationOutput:
        start = time.perf_counter()

        if sql is None or not sql.strip():
            return cls._invalid("no_sql: No SQL provided", start)

        candidate = sql.strip()
        lowered = candidate.lower()

        if cls._has_multiple_statements(candidate):
            return cls._invalid("multiple_statements: Multiple SQL statements are not allowed", start)

        if any(re.search(rf"\b{keyword}\b", lowered) for keyword in UNSAFE_SQL_KEYWORDS):
            return cls._invalid("unsafe_keyword: Non-read-only SQL is not allowed", start)

        if not (lowered.startswith("select ") or lowered.startswith("with ")):
            return cls._invalid("not_select: Only SELECT queries are allowed", start)

        if question and cls._question_has_unsupported_concept(question):
            return cls._invalid("unsupported_concept: Question asks for data not present in the schema", start)

        unknown_table = cls._find_unknown_table(candidate)
        if unknown_table:
            return cls._invalid(f"unknown_table: Unsupported table `{unknown_table}`", start)

        compile_error = cls._compile_sql(candidate)
        if compile_error:
            if "no such column" in compile_error.lower():
                return cls._invalid(f"unknown_column: {compile_error}", start)
            if "no such table" in compile_error.lower():
                return cls._invalid(f"unknown_table: {compile_error}", start)
            return cls._invalid(f"invalid_sql: {compile_error}", start)

        intent_error = cls._check_question_intent(question or "", lowered)
        if intent_error:
            return cls._invalid(intent_error, start)

        return SQLValidationOutput(
            is_valid=True,
            validated_sql=candidate,
            error=None,
            timing_ms=(time.perf_counter() - start) * 1000,
        )

    @staticmethod
    def _invalid(error: str, start: float) -> SQLValidationOutput:
        return SQLValidationOutput(
            is_valid=False,
            validated_sql=None,
            error=error,
            timing_ms=(time.perf_counter() - start) * 1000,
        )

    @staticmethod
    def _has_multiple_statements(sql: str) -> bool:
        parts = [part.strip() for part in sql.split(";") if part.strip()]
        return len(parts) > 1

    @staticmethod
    def _question_has_unsupported_concept(question: str) -> bool:
        lowered = question.lower()
        return any(keyword in lowered for keyword in UNSUPPORTED_QUESTION_KEYWORDS)

    @staticmethod
    def _find_unknown_table(sql: str) -> str | None:
        tables = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE)
        for table in tables:
            if table.lower() != TABLE_NAME.lower():
                return table
        return None

    @staticmethod
    def _compile_sql(sql: str) -> str | None:
        column_sql = ", ".join(
            [f'"{column}" TEXT' if column == "gender" else f'"{column}" REAL' for column in SCHEMA_COLUMNS]
        )
        try:
            with sqlite3.connect(":memory:") as conn:
                conn.execute(f'CREATE TABLE "{TABLE_NAME}" ({column_sql})')
                conn.execute(f"EXPLAIN QUERY PLAN {sql}")
            return None
        except sqlite3.Error as exc:
            return str(exc)

    @classmethod
    def _check_question_intent(cls, question: str, sql_lower: str) -> str | None:
        lowered = question.lower()
        if not lowered:
            return None

        if any(term in lowered for term in ("average", "avg", "mean")) and "avg(" not in sql_lower:
            return "missing_expected_aggregation: Expected AVG aggregation for average-style question"

        if any(term in lowered for term in ("how many", "count", "number of", "share")) and "count(" not in sql_lower:
            return "missing_expected_aggregation: Expected COUNT aggregation for count/share question"

        if any(term in lowered for term in ("top ", "highest", "lowest", "sort", "largest")) and "order by" not in sql_lower:
            if "max(" not in sql_lower and "min(" not in sql_lower:
                return "missing_expected_ordering: Expected ORDER BY for ranking question"

        if any(term in lowered for term in ("top 5", "top five", "highest", "lowest")) and "limit" not in sql_lower:
            if "max(" not in sql_lower and "min(" not in sql_lower:
                return "missing_expected_limit: Expected LIMIT for bounded ranking question"

        for dimension in SUPPORTED_DIMENSIONS:
            normalized = dimension.replace("_", " ")
            if f"by {normalized}" in lowered or f"across {normalized}" in lowered or f"per {normalized}" in lowered:
                if "group by" not in sql_lower:
                    return f"missing_expected_grouping: Expected GROUP BY for `{dimension}` breakdown"

        return None


class SQLiteExecutor:
    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)

    def run(self, sql: str | None) -> SQLExecutionOutput:
        start = time.perf_counter()
        error = None
        rows: list[dict[str, Any]] = []
        row_count = 0

        if sql is None:
            return SQLExecutionOutput(
                rows=[],
                row_count=0,
                timing_ms=(time.perf_counter() - start) * 1000,
                error=None,
            )

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchmany(100)]
                row_count = len(rows)
        except Exception as exc:
            error = str(exc)
            rows = []
            row_count = 0

        return SQLExecutionOutput(
            rows=rows,
            row_count=row_count,
            timing_ms=(time.perf_counter() - start) * 1000,
            error=error,
        )


class AnalyticsPipeline:
    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH, llm_client: OpenRouterLLMClient | None = None) -> None:
        self.db_path = Path(db_path)
        self._ensure_database_ready()
        self.llm = llm_client or build_default_llm_client()
        self.executor = SQLiteExecutor(self.db_path)

    @staticmethod
    def is_follow_up_question(question: str) -> bool:
        lowered = question.strip().lower()
        return any(pattern in lowered for pattern in FOLLOW_UP_PATTERNS)

    @staticmethod
    def _question_requests_unsafe_action(question: str) -> bool:
        lowered = question.lower()
        return any(keyword in lowered for keyword in ("delete", "drop", "update", "insert", "alter"))

    @staticmethod
    def _question_has_unsupported_concept(question: str) -> bool:
        return SQLValidator._question_has_unsupported_concept(question)

    @staticmethod
    def _resolve_measure(question: str) -> str | None:
        lowered = question.lower()
        average_match = re.search(r"(?:average|avg|mean)\s+(.+?)\s+(?:by|across|per)\b", lowered)
        if average_match:
            measure_hint = average_match.group(1)
            for phrase, column in sorted(MEASURE_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
                if re.search(rf"\b{re.escape(phrase)}\b", measure_hint):
                    return column
        for phrase, column in sorted(MEASURE_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
            if re.search(rf"\b{re.escape(phrase)}\b", lowered):
                return column
        return None

    @staticmethod
    def _resolve_dimension(question: str) -> str | None:
        lowered = question.lower()
        dimension_match = re.search(r"\b(?:by|across|per)\s+(.+)", lowered)
        if dimension_match:
            dimension_hint = dimension_match.group(1)
            for phrase, column in sorted(DIMENSION_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
                if re.search(rf"\b{re.escape(phrase)}\b", dimension_hint):
                    return column
        for phrase, column in sorted(DIMENSION_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
            if re.search(rf"\b{re.escape(phrase)}\b", lowered):
                return column
        if "genders" in lowered:
            return "gender"
        return None

    @staticmethod
    def generate_heuristic_sql(question: str) -> str | None:
        lowered = question.lower()

        if "age group" in lowered and "addiction level" in lowered:
            measure = "addiction_level"
            dimension = "age"
        elif "age group" in lowered and "anxiety score" in lowered:
            measure = "anxiety_score"
            dimension = "age"
        elif any(term in lowered for term in ("gender", "genders")) and "addiction level" in lowered:
            measure = "addiction_level"
            dimension = "gender"
        elif any(term in lowered for term in ("gender", "genders")) and "anxiety score" in lowered:
            measure = "anxiety_score"
            dimension = "gender"
        elif "addiction level" in lowered and "anxiety score" in lowered:
            measure = "anxiety_score"
            dimension = "addiction_level"
        else:
            measure = AnalyticsPipeline._resolve_measure(question)
            dimension = AnalyticsPipeline._resolve_dimension(question)

        top_match = re.search(r"top\s+(\d+)", lowered)
        if top_match and measure and dimension:
            limit = int(top_match.group(1))
            alias = f"avg_{measure}"
            return (
                f"SELECT {dimension}, AVG({measure}) AS {alias} "
                f"FROM {TABLE_NAME} GROUP BY {dimension} ORDER BY {alias} DESC LIMIT {limit}"
            )

        if "top five" in lowered and measure and dimension:
            alias = f"avg_{measure}"
            return (
                f"SELECT {dimension}, AVG({measure}) AS {alias} "
                f"FROM {TABLE_NAME} GROUP BY {dimension} ORDER BY {alias} DESC LIMIT 5"
            )

        if any(term in lowered for term in ("average", "avg", "mean")) and measure and dimension:
            alias = f"avg_{measure}"
            order_clause = f" ORDER BY {alias} DESC" if any(term in lowered for term in ("highest", "top", "lowest")) else ""
            limit_clause = " LIMIT 1" if any(term in lowered for term in ("highest", "lowest")) else ""
            direction = "ASC" if "lowest" in lowered else "DESC"
            if order_clause:
                order_clause = f" ORDER BY {alias} {direction}"
            return (
                f"SELECT {dimension}, AVG({measure}) AS {alias} "
                f"FROM {TABLE_NAME} GROUP BY {dimension}{order_clause}{limit_clause}"
            )

        if any(term in lowered for term in ("vary between", "compare", "across")) and measure and dimension:
            alias = f"avg_{measure}"
            return (
                f"SELECT {dimension}, AVG({measure}) AS {alias} "
                f"FROM {TABLE_NAME} GROUP BY {dimension} ORDER BY {alias} DESC"
            )

        if any(term in lowered for term in ("highest", "lowest")) and measure and dimension:
            alias = f"avg_{measure}"
            direction = "ASC" if "lowest" in lowered else "DESC"
            limit = " LIMIT 1" if f"which {dimension.replace('_', ' ')}" in lowered else " LIMIT 5"
            return (
                f"SELECT {dimension}, AVG({measure}) AS {alias} "
                f"FROM {TABLE_NAME} GROUP BY {dimension} ORDER BY {alias} {direction}{limit}"
            )

        if any(term in lowered for term in ("how many", "count", "number of")) and "high addiction" in lowered:
            return (
                f"SELECT COUNT(*) AS high_addiction_count FROM {TABLE_NAME} "
                "WHERE addiction_level >= 5"
            )

        if "share" in lowered and "low addiction" in lowered:
            return (
                f"SELECT ROUND(100.0 * SUM(CASE WHEN addiction_level < 2 THEN 1 ELSE 0 END) / COUNT(*), 2) "
                f"AS low_addiction_share FROM {TABLE_NAME}"
            )

        if "anxiety" in lowered and "as addiction level increases" in lowered:
            return (
                f"SELECT addiction_level, AVG(anxiety_score) AS avg_anxiety "
                f"FROM {TABLE_NAME} GROUP BY addiction_level ORDER BY addiction_level"
            )

        if "younger respondents" in lowered and "older respondents" in lowered:
            return (
                f"SELECT age, AVG(addiction_level) AS avg_addiction_level "
                f"FROM {TABLE_NAME} GROUP BY age ORDER BY age"
            )

        return None

    def _ensure_database_ready(self) -> None:
        needs_bootstrap = not self.db_path.exists() or self.db_path.stat().st_size == 0
        if not needs_bootstrap:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    row = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (TABLE_NAME,),
                    ).fetchone()
                needs_bootstrap = row is None
            except sqlite3.Error:
                needs_bootstrap = True

        if needs_bootstrap:
            if not DEFAULT_CSV_PATH.exists():
                raise RuntimeError(
                    f"Database at {self.db_path} is not initialized and source CSV is missing at {DEFAULT_CSV_PATH}."
                )
            csv_to_sqlite(DEFAULT_CSV_PATH, self.db_path, TABLE_NAME, if_exists="replace")

    def _build_sql_context(
        self,
        question: str,
        conversation_history: list[TurnContext] | None,
    ) -> dict[str, Any]:
        context: dict[str, Any] = {"table": TABLE_NAME, "columns": list(SCHEMA_COLUMNS)}
        if conversation_history and self.is_follow_up_question(question):
            last_turn = conversation_history[-1]
            context["follow_up"] = {
                "previous_question": last_turn.question,
                "previous_sql": last_turn.sql,
                "previous_answer": last_turn.answer,
            }
        return context

    def _build_answer_output(
        self,
        *,
        question: str,
        sql: str | None,
        rows: list[dict[str, Any]],
        execution_error: str | None = None,
        validation_error: str | None = None,
    ) -> AnswerGenerationOutput:
        if validation_error:
            return AnswerGenerationOutput(
                answer="I cannot answer this with the available table and schema. Please rephrase using known survey fields.",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")},
                intermediate_outputs=[{"verdict": "rejected", "reason": validation_error}],
                error=None,
            )

        if execution_error:
            return AnswerGenerationOutput(
                answer="I could not produce a reliable answer because the SQL execution failed.",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")},
                intermediate_outputs=[{"verdict": "rejected", "reason": execution_error}],
                error=execution_error,
            )

        if not rows:
            return AnswerGenerationOutput(
                answer="I cannot answer this confidently because the query returned no rows.",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")},
                intermediate_outputs=[{"verdict": "empty_result", "reason": "empty_result"}],
                error=None,
            )

        answer_output = self.llm.generate_answer(question, sql, rows)
        if not answer_output.answer.strip():
            answer_output.answer = "I could not produce a reliable answer from the returned rows."
            answer_output.intermediate_outputs.append({"verdict": "rejected", "reason": "empty_answer"})
        else:
            answer_output.intermediate_outputs.append({"verdict": "accepted", "reason": "grounded_rows"})
        return answer_output

    @staticmethod
    def _combine_llm_stats(
        sql_generation: SQLGenerationOutput,
        answer_generation: AnswerGenerationOutput,
    ) -> dict[str, Any]:
        return {
            "llm_calls": sql_generation.llm_stats.get("llm_calls", 0) + answer_generation.llm_stats.get("llm_calls", 0),
            "prompt_tokens": sql_generation.llm_stats.get("prompt_tokens", 0) + answer_generation.llm_stats.get("prompt_tokens", 0),
            "completion_tokens": sql_generation.llm_stats.get("completion_tokens", 0) + answer_generation.llm_stats.get("completion_tokens", 0),
            "total_tokens": sql_generation.llm_stats.get("total_tokens", 0) + answer_generation.llm_stats.get("total_tokens", 0),
            "model": sql_generation.llm_stats.get("model") or answer_generation.llm_stats.get("model", "unknown"),
        }

    def run(
        self,
        question: str,
        request_id: str | None = None,
        conversation_history: list[TurnContext] | None = None,
    ) -> PipelineOutput:
        request_id = request_id or str(uuid.uuid4())
        start = time.perf_counter()
        log_event("pipeline_started", request_id=request_id, question=question)

        if self._question_requests_unsafe_action(question):
            sql_gen_output = SQLGenerationOutput(
                sql="DELETE FROM gaming_mental_health",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")},
                intermediate_outputs=[{"verdict": "rejected", "reason": "unsafe_request"}],
                error="unsafe_request",
            )
        elif self._question_has_unsupported_concept(question):
            sql_gen_output = SQLGenerationOutput(
                sql=None,
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")},
                intermediate_outputs=[{"verdict": "rejected", "reason": "unsupported_concept"}],
                error="unsupported_concept",
            )
        else:
            sql_context = self._build_sql_context(question, conversation_history)
            sql_gen_output = self.llm.generate_sql(question, sql_context)
            if sql_gen_output.sql is None:
                heuristic_sql = self.generate_heuristic_sql(question)
                if heuristic_sql:
                    sql_gen_output.sql = heuristic_sql
                    sql_gen_output.intermediate_outputs.append(
                        {"verdict": "fallback", "reason": "heuristic_sql_generation"}
                    )
                    if sql_gen_output.error == "OpenRouter response content is not text.":
                        sql_gen_output.error = None

        sql = sql_gen_output.sql
        log_event(
            "sql_generation_completed",
            request_id=request_id,
            sql=sql,
            error=sql_gen_output.error,
            llm_stats=sql_gen_output.llm_stats,
        )

        validation_output = SQLValidator.validate(sql, question=question)
        if not validation_output.is_valid and not self._question_requests_unsafe_action(question):
            heuristic_sql = self.generate_heuristic_sql(question)
            if heuristic_sql and heuristic_sql != sql:
                heuristic_validation = SQLValidator.validate(heuristic_sql, question=question)
                if heuristic_validation.is_valid:
                    sql = heuristic_sql
                    validation_output = heuristic_validation
                    sql_gen_output.sql = heuristic_sql
                    sql_gen_output.intermediate_outputs.append(
                        {"verdict": "fallback", "reason": "heuristic_sql_recovery"}
                    )
        if not validation_output.is_valid:
            sql = None
        log_event(
            "sql_validation_completed",
            request_id=request_id,
            is_valid=validation_output.is_valid,
            error=validation_output.error,
        )

        execution_output = self.executor.run(sql)
        rows = execution_output.rows
        log_event(
            "sql_execution_completed",
            request_id=request_id,
            row_count=execution_output.row_count,
            error=execution_output.error,
        )

        if sql_gen_output.error == "unsupported_concept":
            answer_output = self._build_answer_output(
                question=question,
                sql=None,
                rows=[],
                validation_error="unsupported_concept",
            )
            status = "unanswerable"
        elif not validation_output.is_valid:
            answer_output = self._build_answer_output(
                question=question,
                sql=None,
                rows=[],
                validation_error=validation_output.error,
            )
            status = "invalid_sql"
        elif execution_output.error:
            answer_output = self._build_answer_output(
                question=question,
                sql=sql,
                rows=[],
                execution_error=execution_output.error,
            )
            status = "error"
        elif sql is None:
            answer_output = self._build_answer_output(
                question=question,
                sql=None,
                rows=[],
                validation_error="no_sql",
            )
            status = "unanswerable"
        else:
            answer_output = self._build_answer_output(
                question=question,
                sql=sql,
                rows=rows,
            )
            status = "success"

        log_event(
            "answer_generation_completed",
            request_id=request_id,
            status=status,
            answer_preview=answer_output.answer[:120],
            llm_stats=answer_output.llm_stats,
        )

        timings = {
            "sql_generation_ms": sql_gen_output.timing_ms,
            "sql_validation_ms": validation_output.timing_ms,
            "sql_execution_ms": execution_output.timing_ms,
            "answer_generation_ms": answer_output.timing_ms,
            "total_ms": (time.perf_counter() - start) * 1000,
        }

        total_llm_stats = self._combine_llm_stats(sql_gen_output, answer_output)
        log_event(
            "pipeline_completed",
            request_id=request_id,
            status=status,
            timings=timings,
            total_llm_stats=total_llm_stats,
        )

        return PipelineOutput(
            status=status,
            question=question,
            request_id=request_id,
            sql_generation=sql_gen_output,
            sql_validation=validation_output,
            sql_execution=execution_output,
            answer_generation=answer_output,
            sql=sql,
            rows=rows,
            answer=answer_output.answer,
            timings=timings,
            total_llm_stats=total_llm_stats,
        )


class AnalyticsConversation:
    def __init__(self, pipeline: AnalyticsPipeline) -> None:
        self.pipeline = pipeline
        self.history: list[TurnContext] = []

    def ask(self, question: str, request_id: str | None = None) -> PipelineOutput:
        result = self.pipeline.run(question, request_id=request_id, conversation_history=self.history)
        self.history.append(
            TurnContext(
                question=question,
                sql=result.sql,
                answer=result.answer,
                rows=result.rows,
            )
        )
        return result
