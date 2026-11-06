"""Empty-tool provider compatibility for the separately admitted chat-only lane."""
from types import SimpleNamespace

import pytest

from chatlog_keeper.bot.providers import ChatGPTProvider, ProviderError, validate_tools
from test_bot_required_read import TOOLS, call, compatible, gate, response


def test_global_mcp_schema_guard_still_rejects_empty():
    with pytest.raises(ProviderError, match='bot_unapproved_tools'):
        validate_tools([])


def test_compatible_empty_tools_omits_tools_and_tool_choice():
    bodies = []
    def remote(request):
        import json
        bodies.append(json.loads(request.content))
        return response({'content': 'hello'})
    provider = compatible(remote)
    try:
        assert provider.complete([], 'rules', [], lambda *_: pytest.fail('no tools'), {}) == 'hello'
        assert 'tools' not in bodies[0] and 'tool_choice' not in bodies[0]
        assert bodies[0]['thinking'] == {'type': 'disabled'} and bodies[0]['max_tokens'] == 512
    finally:
        provider.close()


@pytest.mark.parametrize('name', ['query_offer_status', 'report_offer_status', 'fetch_url'])
def test_compatible_empty_tools_rejects_every_remote_tool_before_handler(name):
    provider = compatible(lambda _: response({'tool_calls': [call(name)]}))
    try:
        with pytest.raises(ProviderError, match='bot_unapproved_tool_call'):
            provider.complete([], '', [], lambda *_: pytest.fail('no side effect'), {})
    finally:
        provider.close()


def test_empty_tools_does_not_weaken_required_read_gate():
    provider = compatible(lambda _: pytest.fail('read requirement must fail before HTTP'))
    try:
        with pytest.raises(ProviderError, match='bot_invalid_required_read_tool'):
            provider.complete([], '', [], lambda *_: pytest.fail('no tool'), gate())
    finally:
        provider.close()


@pytest.mark.parametrize('invoke_tool', [False, True])
def test_siwc_empty_tools_are_empty_and_hallucinated_handler_is_blocked(tmp_path, invoke_tool):
    observed = []
    def bot_chat(*args, **kwargs):
        observed.append(kwargs['tools'])
        if invoke_tool:
            kwargs['tool_handler']('report_offer_status', {'item': 'fun'})
        return {'output_text': 'hello'}
    client = SimpleNamespace(state_dir=tmp_path, bot_chat=bot_chat)
    provider = ChatGPTProvider(tmp_path, {'model': 'fixture'}, client=client)
    if invoke_tool:
        with pytest.raises(ProviderError, match='bot_unapproved_tool_call'):
            provider.complete([], '', [], lambda *_: pytest.fail('no side effect'), {})
    else:
        assert provider.complete([], '', [], lambda *_: pytest.fail('no tool'), {}) == 'hello'
    assert observed == [[]] and not (tmp_path / 'bot-chat-waiting').exists()


def test_siwc_nonempty_tools_keep_original_namespace(tmp_path):
    observed = []
    client = SimpleNamespace(state_dir=tmp_path, bot_chat=lambda *args, **kwargs:
        observed.append(kwargs['tools']) or {'output_text': 'ok'})
    provider = ChatGPTProvider(tmp_path, {'model': 'fixture'}, client=client)
    assert provider.complete([], '', TOOLS, lambda *_: {}, {}) == 'ok'
    assert len(observed[0]) == 1 and observed[0][0]['name'] == 'business'
    assert [t['name'] for t in observed[0][0]['tools']] == [t['name'] for t in TOOLS]
