"""One Markdown character source, including lossless legacy conversion."""
import pytest
from pydantic import ValidationError

from chatlog_keeper.bot.persona import default_persona, normalize_persona, persona_prompt
from chatlog_keeper.dashboard.bot_control import BotPersona, BotRuntime


def test_default_and_custom_markdown_are_the_only_character_content_source():
    first, second = default_persona(), default_persona()
    assert set(first) == {"id", "name", "version", "markdown"}
    assert first["markdown"].count("### 示例 ") == 3
    first["markdown"] = "# 新设定\n保持简短\n\n## 示例\n用户：你好\n回复：嗨"
    compiled = persona_prompt(first)
    assert first["markdown"] in compiled and compiled.count("# 新设定") == 1
    assert "没有联网" in compiled and "工具明确返回记录成功" in compiled
    assert second["markdown"] != first["markdown"]
    assert normalize_persona(first) == first


def test_legacy_fields_extra_prompt_and_all_examples_survive_conversion():
    legacy = {"id": "old", "name": "旧角色", "version": "3", "identity": "身份原文",
              "personality": "性格原文", "expression": "表达原文", "scenario": "场景原文",
              "prompt": "# 附加原文\n这段之前曾被遗漏。",
              "examples": [{"user": f"问题{i}", "assistant": f"唯一回复{i}_END"} for i in range(12)]}
    canonical = normalize_persona(legacy)
    assert set(canonical) == {"id", "name", "version", "markdown"}
    assert canonical["version"] == 3
    for field in ("identity", "personality", "expression", "scenario", "prompt"):
        assert legacy[field] in canonical["markdown"]
    assert "唯一回复11_END" in persona_prompt(canonical)
    assert canonical["markdown"].count("### 示例 ") == 12
    assert BotPersona.model_validate(legacy).model_dump() == canonical
    assert normalize_persona(canonical) == canonical


def test_mixed_sources_empty_markdown_and_unknown_fields_fail_explicitly():
    with pytest.raises(ValueError, match="conflict"):
        normalize_persona({**default_persona(), "prompt": "second source"})
    with pytest.raises(ValidationError):
        BotPersona.model_validate({**default_persona(), "markdown": "  \n"})
    with pytest.raises(ValidationError):
        BotPersona.model_validate({**default_persona(), "api_key": "synthetic-secret"})
    with pytest.raises(ValidationError):
        BotPersona.model_validate({**default_persona(), "markdown": "x" * 16001})


def test_runtime_metrics_preserve_unknowns_and_validate_samples():
    value = BotRuntime.model_validate({"active": {"provider": "siwc", "model": "gpt-6.1-sol", "reasoning_effort": "low",
        "persona_id": "alpha", "persona_version": "2"}, "last_request": {"status": "completed", "generation_seconds": 1.2},
        "counts": {"requests": 2, "replied": 1, "unknown": 1},
        "latency_metrics": {"generation": {"count": 2, "p50_seconds": 1, "p95_seconds": 1.2}}}).model_dump()
    assert value["active"]["persona_version"] == 2
    assert value["last_request"]["end_to_end_seconds"] is None
    assert value["counts"]["unknown"] == 1
    assert value["latency_metrics"]["generation"]["count"] == 2
    for invalid in ({"count": 0, "p50_seconds": 1}, {"count": 2, "p50_seconds": 2, "p95_seconds": 1}, {"count": 501}):
        with pytest.raises(ValidationError):
            BotRuntime.model_validate({"latency_metrics": {"generation": invalid}})
    with pytest.raises(ValidationError):
        BotRuntime.model_validate({"latency_metrics": {"arbitrary/private/path": {"count": 1}}})
