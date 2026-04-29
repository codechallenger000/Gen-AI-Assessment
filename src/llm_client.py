from __future__ import annotations

import json
import os
import time
from math import ceil
from typing import Any

from src.types import SQLGenerationOutput, AnswerGenerationOutput
from src.schema import SCHEMA_DESCRIPTION, TABLE_NAME

DEFAULT_MODEL = "openai/gpt-5-nano"


class OpenRouterLLMClient:
    """LLM client using the OpenRouter SDK for chat completions."""

    provider_name = "openrouter"

    def __init__(self, api_key: str, model: str | None = None) -> None:
        try:
            from openrouter import OpenRouter
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency: install 'openrouter'.") from exc
        self.model = model or os.getenv("OPENROUTER_MODEL", DEFAULT_MODEL)
        self._client = OpenRouter(api_key=api_key)
        self._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    def _chat(
        self,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        **request_overrides: Any,
    ) -> str:
        res = self._client.chat.send(
            messages=messages,
            model=self.model,
            temperature=temperature,
            max_completion_tokens=max_tokens,
            stream=False,
            **request_overrides,
        )

        choices = getattr(res, "choices", None) or []
        if not choices:
            raise RuntimeError("OpenRouter response contained no choices.")
        content = getattr(getattr(choices[0], "message", None), "content", None)
        text = self._normalize_content(content)
        self._record_response_usage(res, messages=messages, completion_text=text)
        return text.strip()

    @staticmethod
    def _normalize_content(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                    continue
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)
            if parts:
                return "".join(parts)
        raise RuntimeError("OpenRouter response content is not text.")

    @staticmethod
    def _approximate_token_count(text: str) -> int:
        if not text:
            return 0
        return max(1, ceil(len(text) / 4))

    def _record_response_usage(
        self,
        response: Any,
        *,
        messages: list[dict[str, str]],
        completion_text: str,
    ) -> None:
        usage = getattr(response, "usage", None)
        prompt_tokens = self._read_usage_value(usage, "prompt_tokens", "input_tokens")
        completion_tokens = self._read_usage_value(usage, "completion_tokens", "output_tokens")
        total_tokens = self._read_usage_value(usage, "total_tokens")

        if prompt_tokens is None:
            prompt_tokens = self._approximate_token_count(
                "\n".join(message.get("content", "") for message in messages)
            )
        if completion_tokens is None:
            completion_tokens = self._approximate_token_count(completion_text)
        if total_tokens is None:
            total_tokens = prompt_tokens + completion_tokens

        self._stats["llm_calls"] += 1
        self._stats["prompt_tokens"] += int(prompt_tokens)
        self._stats["completion_tokens"] += int(completion_tokens)
        self._stats["total_tokens"] += int(total_tokens)

    @staticmethod
    def _read_usage_value(usage: Any, *names: str) -> int | None:
        if usage is None:
            return None
        for name in names:
            value = getattr(usage, name, None)
            if isinstance(value, int):
                return value
            if isinstance(usage, dict) and isinstance(usage.get(name), int):
                return usage[name]
        return None

    @staticmethod
    def _extract_sql(text: str) -> str | None:
        maybe_json = text.strip()
        if maybe_json.startswith("{") and maybe_json.endswith("}"):
            try:
                parsed = json.loads(maybe_json)
                sql = parsed.get("sql")
                if isinstance(sql, str) and sql.strip():
                    return sql.strip()
                return None
            except json.JSONDecodeError:
                pass
        lower = text.lower()
        idx = lower.find("select ")
        if idx >= 0:
            return text[idx:].strip()
        return None

    def generate_sql(self, question: str, context: dict) -> SQLGenerationOutput:
        system_prompt = (
            "You are a SQL assistant for a single-table SQLite analytics dataset. "
            "Generate exactly one safe SELECT query when the question is answerable from the schema. "
            "If the question cannot be answered from the schema, return JSON with sql set to null. "
            "Never use DELETE, UPDATE, INSERT, DROP, ALTER, PRAGMA, ATTACH, or multiple statements."
        )
        user_prompt = (
            f"{SCHEMA_DESCRIPTION}\n\n"
            f"Conversation context: {json.dumps(context, ensure_ascii=True)}\n\n"
            f"Question: {question}\n\n"
            "Return strict JSON only, in one of these forms:\n"
            '{"sql": "SELECT ..."}\n'
            '{"sql": null, "reason": "unsupported"}\n\n'
            f"Rules:\n"
            f"- Query only the `{TABLE_NAME}` table.\n"
            "- Prefer aggregate answers when asked for averages, counts, shares, top values, or comparisons.\n"
            "- Use LIMIT for top/bottom questions.\n"
            "- Use only columns from the provided schema.\n"
            "- Do not include markdown fences or explanations."
        )

        start = time.perf_counter()
        error = None
        sql = None

        try:
            text = self._chat(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.0,
                max_tokens=240,
                response_format={"type": "json_object"},
                reasoning={"effort": "low"},
            )
            sql = self._extract_sql(text)
        except Exception as exc:
            error = str(exc)

        timing_ms = (time.perf_counter() - start) * 1000
        llm_stats = self.pop_stats()
        llm_stats["model"] = self.model

        return SQLGenerationOutput(
            sql=sql,
            timing_ms=timing_ms,
            llm_stats=llm_stats,
            error=error,
        )

    def generate_answer(self, question: str, sql: str | None, rows: list[dict[str, Any]]) -> AnswerGenerationOutput:
        if not sql:
            return AnswerGenerationOutput(
                answer="I cannot answer this with the available table and schema. Please rephrase using known survey fields.",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": self.model},
                error=None,
            )
        if not rows:
            return AnswerGenerationOutput(
                answer="Query executed, but no rows were returned.",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": self.model},
                error=None,
            )

        system_prompt = (
            "You are a concise analytics assistant. "
            "Use only the provided SQL results. Do not invent data. "
            "If the rows are insufficient to support a claim, say that the result is inconclusive."
        )
        user_prompt = (
            f"Question:\n{question}\n\nSQL:\n{sql}\n\n"
            f"Rows (JSON):\n{json.dumps(rows[:30], ensure_ascii=True)}\n\n"
            "Write a concise answer in plain English grounded only in the rows."
        )

        start = time.perf_counter()
        error = None
        answer = ""

        try:
            answer = self._chat(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.2,
                max_tokens=220,
                reasoning={"effort": "low"},
            )
        except Exception as exc:
            error = str(exc)
            answer = self._fallback_answer(question, rows)

        timing_ms = (time.perf_counter() - start) * 1000
        llm_stats = self.pop_stats()
        llm_stats["model"] = self.model

        return AnswerGenerationOutput(
            answer=answer,
            timing_ms=timing_ms,
            llm_stats=llm_stats,
            error=error,
        )

    def pop_stats(self) -> dict[str, Any]:
        out = dict(self._stats or {})
        self._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        return out

    @staticmethod
    def _fallback_answer(question: str, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "I cannot answer this confidently because the query returned no rows."

        if len(rows) == 1:
            row = rows[0]
            if len(row) == 1:
                key, value = next(iter(row.items()))
                label = key.replace("_", " ")
                return f"The {label} is {value}."
            parts = [f"{key.replace('_', ' ')}={value}" for key, value in row.items()]
            return "The result is: " + ", ".join(parts) + "."

        preview = rows[:5]
        if len(preview[0]) == 2:
            items: list[str] = []
            for row in preview:
                keys = list(row.keys())
                items.append(f"{row[keys[0]]}: {row[keys[1]]}")
            return "Here are the leading results: " + "; ".join(items) + "."

        return (
            f"I could not generate a narrative answer for the question `{question}`, "
            "but the SQL executed successfully and returned structured rows."
        )


def build_default_llm_client() -> OpenRouterLLMClient:
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required.")
    return OpenRouterLLMClient(api_key=api_key)
