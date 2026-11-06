"""Provider resource ownership, using only injected in-memory fakes."""
import copy
from pathlib import Path
import threading

import pytest

from chatlog_keeper.bot.protocol_runtime import ProviderResources
from chatlog_keeper.bot.providers import ProviderError, profile_signature


def profile(kind="compatible", **changes):
    value = {"id": "synthetic", "kind": kind, "model": "synthetic-model",
             "base_url": "https://synthetic.invalid/v1",
             "credential_ref": "synthetic-key", "reasoning_effort": "low"}
    value.update(changes)
    return value


class FakeHTTP:
    def __init__(self, args, kwargs):
        self.args, self.kwargs = args, kwargs
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class FakeActor:
    def __init__(self, directory, *, fail_close=False):
        self.directory = directory
        self.fail_close = fail_close
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        if self.fail_close:
            raise RuntimeError("synthetic_usage_close_failure")


class FakeProvider:
    def __init__(self, kind):
        self.kind = kind
        self.close_calls = 0
        self.completions = []

    def complete(self, *args, **kwargs):
        self.completions.append((args, kwargs))
        return "synthetic answer"

    def close(self):
        self.close_calls += 1


class Fakes:
    def __init__(self, tmp_path):
        self.store = {}
        self.state_dir = tmp_path / "state"
        self.usage_dir = tmp_path / "usage"
        self.dashboard_dir = tmp_path / "dashboard"
        self.clients, self.actors, self.providers, self.calls = [], [], [], []
        self.fail_kind = None
        self.fail_actor_close = False
        self.resources = ProviderResources(
            self.store, state_dir=self.state_dir, usage_dir=self.usage_dir,
            dashboard_dir=self.dashboard_dir, http_factory=self.http_factory,
            compatible_factory=self.compatible_factory,
            chatgpt_factory=self.chatgpt_factory, usage_factory=self.usage_factory)

    def http_factory(self, *args, **kwargs):
        client = FakeHTTP(args, kwargs)
        self.clients.append(client)
        return client

    def usage_factory(self, directory):
        actor = FakeActor(directory, fail_close=self.fail_actor_close)
        self.actors.append(actor)
        return actor

    def compatible_factory(self, state_dir, usage_dir, settings, *, client, ledger):
        self.calls.append({"kind": "compatible", "state_dir": state_dir,
                           "usage_dir": usage_dir, "profile": settings,
                           "client": client, "ledger": ledger})
        return self._provider("compatible")

    def chatgpt_factory(self, dashboard_dir, settings, *, usage_dir, http_client):
        self.calls.append({"kind": "siwc", "dashboard_dir": dashboard_dir,
                           "usage_dir": usage_dir, "profile": settings,
                           "client": http_client})
        return self._provider("siwc")

    def _provider(self, kind):
        if self.fail_kind == kind:
            raise RuntimeError("synthetic_provider_init_failure")
        provider = FakeProvider(kind)
        self.providers.append(provider)
        return provider


def close_in_thread(resources):
    entered, finished, errors = threading.Event(), threading.Event(), []

    def close():
        entered.set()
        try:
            resources.close()
        except Exception as error:
            errors.append(error)
        finally:
            finished.set()

    thread = threading.Thread(target=close, daemon=True)
    thread.start()
    assert entered.wait(1), "resource closer did not start"
    return thread, finished, errors


def test_shared_http_and_actor_preserve_profiles_and_delegate_complete(tmp_path):
    fake = Fakes(tmp_path)
    settings = [profile(), profile(id="second", model="another-model"),
                profile(id="other-endpoint", base_url="https://other.invalid/v1"),
                profile("siwc", id="subscription")]
    originals = copy.deepcopy(settings)
    borrowed = []
    try:
        for value in settings:
            borrowed.append(fake.resources.provider(value))
        first, second, other, subscription = fake.calls
        assert first["client"] is second["client"]
        assert other["client"] is not first["client"]
        assert subscription["client"] not in (first["client"], other["client"])
        assert len(fake.clients) == 3
        assert len(fake.actors) == 1
        assert first["ledger"] is second["ledger"] is other["ledger"] is fake.actors[0]
        assert Path(first["state_dir"]) == fake.state_dir
        assert Path(first["usage_dir"]) == fake.usage_dir
        assert Path(fake.actors[0].directory) == fake.usage_dir
        assert Path(subscription["dashboard_dir"]) == fake.dashboard_dir
        assert Path(subscription["usage_dir"]) == fake.usage_dir
        assert all(call["profile"].get("tools_verified") is False for call in fake.calls[:3])
        assert settings == originals
        args = ([{"role": "user", "content": "synthetic"}], "rules", [], object(), {})
        assert borrowed[0].complete(*args) == "synthetic answer"
        assert fake.providers[0].completions == [(args, {})]
        borrowed[0].close()
        borrowed[0].close()
        assert fake.providers[0].close_calls == 1
        assert all(client.close_calls == 0 for client in fake.clients)
        assert fake.actors[0].close_calls == 0
    finally:
        for provider in borrowed:
            provider.close()
        fake.resources.close()
    fake.resources.close()
    assert all(provider.close_calls == 1 for provider in fake.providers)
    assert all(client.close_calls == 1 for client in fake.clients)
    assert fake.actors[0].close_calls == 1


@pytest.mark.parametrize("verified", [False, True])
def test_tools_verification_is_derived_from_exact_profile_signature(tmp_path, verified):
    fake = Fakes(tmp_path)
    settings = profile(tools_verified=not verified)
    original = copy.deepcopy(settings)
    fake.store["verified_profile_" + settings["id"]] = (
        profile_signature(settings) if verified else profile_signature(profile(model="outdated")))
    borrowed = fake.resources.provider(settings)
    try:
        assert fake.calls[0]["profile"]["tools_verified"] is verified
        assert settings == original
    finally:
        borrowed.close()
        fake.resources.close()


def test_close_waits_for_both_provider_kinds_and_rejects_new_borrowers(tmp_path):
    fake = Fakes(tmp_path)
    compatible = fake.resources.provider(profile())
    subscription = fake.resources.provider(profile("siwc", id="subscription"))
    thread, finished, errors = close_in_thread(fake.resources)
    try:
        assert not finished.wait(.05), "resources closed while both leases were active"
        before = len(fake.calls)
        with pytest.raises(ProviderError, match="^bot_provider_resources_closed$"):
            fake.resources.provider(profile(id="too-late"))
        assert len(fake.calls) == before
        assert all(client.close_calls == 0 for client in fake.clients)
        assert fake.actors[0].close_calls == 0
        compatible.close()
        compatible.close()
        assert not finished.wait(.05), "the subscription provider still owns a lease"
        assert all(client.close_calls == 0 for client in fake.clients)
        assert fake.actors[0].close_calls == 0
    finally:
        compatible.close()
        subscription.close()
        thread.join(2)
    assert finished.is_set(), "resource close did not finish after both leases returned"
    assert not errors
    assert all(provider.close_calls == 1 for provider in fake.providers)
    assert all(client.close_calls == 1 for client in fake.clients)
    assert fake.actors[0].close_calls == 1
    with pytest.raises(ProviderError, match="^bot_provider_resources_closed$"):
        fake.resources.provider(profile())
    fake.resources.close()


@pytest.mark.parametrize("kind", ["compatible", "siwc"])
def test_provider_constructor_failure_releases_lease(tmp_path, kind):
    fake = Fakes(tmp_path)
    fake.fail_kind = kind
    with pytest.raises(RuntimeError, match="^synthetic_provider_init_failure$"):
        fake.resources.provider(profile(kind))
    fake.fail_kind = None
    borrowed = fake.resources.provider(profile(kind))
    assert borrowed.complete() == "synthetic answer"
    borrowed.close()
    thread, finished, errors = close_in_thread(fake.resources)
    thread.join(2)
    assert finished.is_set(), "failed construction leaked a provider lease"
    assert not errors
    assert all(client.close_calls == 1 for client in fake.clients)
    assert all(actor.close_calls == 1 for actor in fake.actors)


def test_actor_close_failure_still_closes_every_shared_http_client(tmp_path):
    fake = Fakes(tmp_path)
    fake.fail_actor_close = True
    borrowed = [fake.resources.provider(profile()),
                fake.resources.provider(profile(id="second", base_url="https://other.invalid/v1")),
                fake.resources.provider(profile("siwc", id="subscription"))]
    for provider in borrowed:
        provider.close()
    thread, finished, errors = close_in_thread(fake.resources)
    thread.join(2)
    assert finished.is_set(), "a failed actor close must not leave shutdown waiting"
    assert len(errors) <= 1
    if errors:
        assert str(errors[0]) == "synthetic_usage_close_failure"
    assert fake.actors[0].close_calls == 1
    assert len(fake.clients) == 3
    assert all(client.close_calls == 1 for client in fake.clients)
    with pytest.raises(ProviderError, match="^bot_provider_resources_closed$"):
        fake.resources.provider(profile(id="after-failed-close"))
