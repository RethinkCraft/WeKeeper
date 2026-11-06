import io
import json
import os
import stat
import threading

import httpx
from PIL import Image
import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.dashboard.avatar_backfill import AvatarBackfill, AvatarError, allowed_url, _fetch
from test_dashboard_sync import archive_fixture

URL = 'https://wx.qlogo.cn/mmhead/example/132'


def image_bytes(fmt='PNG'):
    out = io.BytesIO()
    Image.new('RGB', (16, 12), 'blue').save(out, format=fmt)
    return out.getvalue()


@pytest.fixture
def setup(tmp_path, monkeypatch):
    archive, group = archive_fixture(tmp_path / 'archive')
    state = tmp_path / 'dashboard'
    state.mkdir()
    config = state / 'config.json'
    config.write_text(json.dumps({'groups': [group]}))
    clock = [1000.0]
    sources = {'wxid_private': {'small': URL, 'big': '', 'version': 'a'}}
    requests = []
    result = [lambda request: httpx.Response(200, content=image_bytes())]
    def transport(request):
        requests.append(request)
        return result[0](request)
    client = httpx.Client(transport=httpx.MockTransport(transport))
    worker = AvatarBackfill(tmp_path / 'archive', state, client=client, clock=lambda: clock[0])
    monkeypatch.setattr(worker, '_contacts', lambda account, subjects: {key: value for key, value in sources.items() if key in subjects})
    yield worker, archive, group, config, clock, sources, requests, result
    worker.close()
    client.close()
    archive.close()


@pytest.mark.parametrize('url', [URL, 'https://wx.qlogo.cn:443/a', 'HTTPS://WX.QLOGO.CN/a?x=1'])
def test_allowed_urls(url):
    assert allowed_url(url)


@pytest.mark.parametrize('url', [
    'http://wx.qlogo.cn/a', 'https://other.example/a', 'https://wx.qlogo.cn.evil.example/a',
    'https://user@wx.qlogo.cn/a', 'https://@wx.qlogo.cn/a', 'https://wx.qlogo.cn:80/a',
    'https://wx.qlogo.cn:bad/a', 'https://wx.qlogo.cn/a#fragment', 'https://wx.qlogo.cn/a#',
    'https://wx.qlogo.cn/a\n', 'https://wx.qlogo.cn/a\\path', 'file:///tmp/a',
])
def test_disallowed_urls(url):
    assert not allowed_url(url)


def test_identity_scope_download_and_restart_dedup(setup):
    worker, archive, group, config, clock, sources, requests, _ = setup
    other = archive.conn.execute('SELECT group_ref FROM groups WHERE group_ref<>?', (group,)).fetchone()[0]
    archive.conn.execute('INSERT INTO group_members VALUES(?,?,?)', (group, 'quiet_member', json.dumps({'display_name': 'Quiet'})))
    archive.conn.execute('INSERT INTO group_members VALUES(?,?,?)', (other, 'not_allowed', '{}'))
    archive.conn.commit()
    sources['quiet_member'] = {'small': URL + '?quiet', 'big': '', 'version': 'a'}
    sources['not_allowed'] = {'small': URL + '?excluded', 'big': '', 'version': 'a'}
    assert worker.scan()['counts'] == {'pending': 2}
    # Prevent any real pacing wait in this two-item test.
    worker.flush(limit=1)
    clock[0] += 2
    assert worker.flush()['counts'] == {'ready': 2}
    assert len(requests) == 2
    rows = archive.conn.execute('SELECT subject_id,payload,avatar_ref FROM profiles').fetchall()
    assert {row[0] for row in rows} == {'wxid_private', 'quiet_member'}
    for row in rows:
        assert 'qlogo' not in row['payload']
        value = archive.get_profile_avatar(row['avatar_ref'])
        with Image.open(io.BytesIO(value['data'])) as image:
            assert image.format == 'JPEG' and image.size == (16, 12) and not image.getexif()
    assert 'qlogo' not in json.dumps(worker.status()) and 'wxid' not in json.dumps(worker.status())
    reopened = AvatarBackfill(worker.archive_dir, worker.directory, client=worker.client, clock=lambda: clock[0])
    try:
        assert reopened.flush()['counts'] == {'ready': 2}
        assert len(requests) == 2
        assert stat.S_IMODE((worker.directory / 'avatar-backfill.sqlite').stat().st_mode) == 0o600
    finally:
        reopened.close()


def test_revoked_whitelist_prevents_download_and_removes_private_candidate(setup):
    worker, _, _, config, _, _, requests, _ = setup
    worker.scan()
    config.write_text(json.dumps({'groups': []}))
    assert worker.flush()['total'] == 0
    assert not requests


def test_active_members_are_filled_before_silent_roster_members(setup):
    worker, archive, group, _, _, sources, requests, _ = setup
    archive.conn.execute('UPDATE messages SET create_time=900')
    archive.conn.execute('INSERT INTO group_members VALUES(?,?,?)', (group, 'aaa_quiet', '{}'))
    archive.conn.commit()
    sources['aaa_quiet'] = {'small': URL + '?quiet', 'big': '', 'version': 'a'}
    worker.scan()
    worker.flush(limit=1)
    assert len(requests) == 1
    assert archive.conn.execute('SELECT subject_id FROM profiles').fetchone()[0] == 'wxid_private'


def test_revocation_during_fetch_does_not_archive(setup):
    worker, archive, _, config, _, _, requests, response = setup
    worker.scan()
    def revoke(request):
        config.write_text(json.dumps({'groups': []}))
        return httpx.Response(200, content=image_bytes())
    response[0] = revoke
    worker.flush()
    assert len(requests) == 1
    assert archive.conn.execute('SELECT COUNT(*) FROM profiles').fetchone()[0] == 0


def test_network_retry_persists_then_recovers(setup):
    worker, _, _, _, clock, _, requests, response = setup
    def failure(request):
        raise httpx.ConnectError('do not expose ' + URL)
    response[0] = failure
    worker.scan()
    assert worker.flush()['counts'] == {'retry': 1}
    worker.scan()
    worker.flush()
    assert len(requests) == 1
    reopened = AvatarBackfill(worker.archive_dir, worker.directory, client=worker.client, clock=lambda: clock[0])
    try:
        assert reopened.flush()['counts'] == {'retry': 1}
        clock[0] += 61
        response[0] = lambda request: httpx.Response(200, content=image_bytes())
        assert reopened.flush()['counts'] == {'ready': 1}
        assert len(requests) == 2
    finally:
        reopened.close()


def test_expired_address_waits_for_new_version(setup):
    worker, _, _, _, clock, sources, requests, response = setup
    response[0] = lambda request: httpx.Response(403)
    worker.scan()
    assert worker.flush()['counts'] == {'unavailable': 1}
    clock[0] += 10000
    worker.scan()
    worker.flush()
    assert len(requests) == 1
    sources['wxid_private']['version'] = 'b'
    worker.scan()
    assert worker.status()['counts'] == {'pending': 1}
    response[0] = lambda request: httpx.Response(200, content=image_bytes())
    assert worker.flush()['counts'] == {'ready': 1}
    assert len(requests) == 2


def test_missing_url_and_invalid_url_do_not_fetch(setup):
    worker, _, _, _, _, sources, requests, _ = setup
    sources['wxid_private'] = {'small': 'http://wx.qlogo.cn/x', 'big': 'https://evil.test/x', 'version': 'a'}
    assert worker.scan()['counts'] == {'missing_url': 1}
    worker.flush()
    assert not requests


@pytest.mark.parametrize('status,headers,body,error', [
    (302, {'location': 'https://evil.test/a'}, b'', 'avatar_redirect_rejected'),
    (200, {'content-length': str(4 * 1024 * 1024 + 1)}, b'', 'avatar_too_large'),
    (200, {}, b'<html>fake image</html>', 'invalid_avatar_image'),
    (200, {'content-type': 'image/jpeg'}, b'not an image', 'invalid_avatar_image'),
])
def test_invalid_responses_are_permanent_and_never_saved(setup, status, headers, body, error):
    worker, archive, _, _, _, _, requests, response = setup
    response[0] = lambda request: httpx.Response(status, headers=headers, content=body)
    worker.scan()
    assert worker.flush()['counts'] == {'unavailable': 1}
    assert len(requests) == 1
    assert worker.conn.execute('SELECT error FROM jobs').fetchone()[0] == error
    assert archive.conn.execute('SELECT COUNT(*) FROM avatar_assets').fetchone()[0] == 0


def test_stream_size_and_pixel_limits(setup):
    worker, _, _, _, _, _, _, response = setup
    response[0] = lambda request: httpx.Response(200, content=b'x' * (4 * 1024 * 1024 + 1))
    worker.scan()
    assert worker.flush()['counts'] == {'unavailable': 1}
    out = io.BytesIO()
    Image.new('RGB', (2001, 2000)).save(out, format='PNG')
    from chatlog_keeper.dashboard.avatar_backfill import _image_bytes
    with pytest.raises(AvatarError, match='invalid_avatar_image'):
        _image_bytes(out.getvalue())


def test_existing_valid_avatar_prevents_request_and_failure_preserves_it(setup):
    worker, archive, _, _, _, _, requests, _ = setup
    worker.scan()
    worker.flush()
    original = archive.conn.execute('SELECT avatar_ref FROM profiles').fetchone()[0]
    # Simulate queued work created before a collector supplied this good image.
    worker.conn.execute("UPDATE jobs SET status='pending'")
    worker.conn.commit()
    worker.flush()
    worker.scan()
    assert len(requests) == 1
    assert archive.conn.execute('SELECT avatar_ref FROM profiles').fetchone()[0] == original


def test_stop_prevents_request(setup):
    worker, _, _, _, _, _, requests, _ = setup
    worker.scan()
    stop = threading.Event()
    stop.set()
    assert worker.flush(stop=stop)['counts'] == {'pending': 1}
    assert not requests


def test_limited_retry_exhaustion_survives_scan(setup):
    worker, _, _, _, clock, _, requests, response = setup
    response[0] = lambda request: httpx.Response(503)
    worker.scan()
    for _ in range(5):
        worker.flush()
        clock[0] += 4000
        worker.scan()
    assert worker.status()['counts'] == {'unavailable': 1}
    worker.flush()
    assert len(requests) == 5


def test_authenticated_contacts_only_read_exact_selected_identity(setup, tmp_path, monkeypatch):
    worker, archive, _, _, _, _, _, _ = setup
    from chatlog_keeper.collector import Collector
    from chatlog_keeper.p2_sources import P2Sources
    import sqlite3
    root = tmp_path / 'source'
    root.mkdir()
    archive.set_collector_status(account_id='private_account', data_root=str(root))
    plain = tmp_path / 'contact-plaintext.sqlite'
    with sqlite3.connect(plain) as source:
        source.execute('CREATE TABLE contact(username,small_head_url,big_head_url,head_img_md5)')
        source.executemany('INSERT INTO contact VALUES(?,?,?,?)', [
            ('wxid_private', URL, '', 'known'), ('outside', 'https://do-not-read.test', '', 'outside')])
    checked = []
    monkeypatch.setattr(P2Sources, '_safe_source', lambda self, path: None)
    def key(self, reader, path):
        assert reader.enc_key is None and reader.enc_keys == {}
        assert self.collector.account_id == 'private_account'
        checked.append('cached-key-verification')
        return b'not-a-real-key'
    monkeypatch.setattr(P2Sources, '_key', key)
    monkeypatch.setattr(Collector, '_fingerprint', staticmethod(lambda path: 'fingerprint'))
    monkeypatch.setattr(Collector, '_get_snapshot', lambda self, path, key, fingerprint: plain)
    monkeypatch.setattr(Collector, '_initialize', lambda *args: pytest.fail('must not initialize or extract keys'), raising=False)
    result = AvatarBackfill._contacts(worker, 'private_account', {'wxid_private'})
    assert checked == ['cached-key-verification']
    assert set(result) == {'wxid_private'} and result['wxid_private']['small'] == URL
    with pytest.raises(AvatarError, match='account_unavailable'):
        AvatarBackfill._contacts(worker, '../private_account', {'wxid_private'})


def test_parallel_workers_cannot_download_concurrently(setup):
    worker, _, _, _, _, _, _, _ = setup
    other = AvatarBackfill(worker.archive_dir, worker.directory, client=worker.client)
    try:
        with worker._exclusive():
            with pytest.raises(AvatarError, match='already_running'):
                other.flush()
    finally:
        other.close()


def test_cache_reencoding_strips_exif_and_webp_is_supported():
    from chatlog_keeper.dashboard.avatar_backfill import _image_bytes
    original = Image.new('RGB', (10, 10), 'red')
    exif = Image.Exif()
    exif[270] = 'private metadata'
    out = io.BytesIO()
    original.save(out, format='JPEG', exif=exif)
    body, _, _ = _image_bytes(out.getvalue())
    with Image.open(io.BytesIO(body)) as image:
        assert not image.getexif() and b'private metadata' not in body
    body, _, _ = _image_bytes(image_bytes('WEBP'))
    with Image.open(io.BytesIO(body)) as image:
        assert image.format == 'JPEG'


@pytest.mark.parametrize('failure_kind', ['network', 'timeout', 'server', 'ratelimit'])
def test_exhausted_temporary_failures_retry_after_six_hours(setup, failure_kind):
    worker, _, _, _, clock, _, requests, response = setup
    def fail(request):
        if failure_kind == 'network':
            raise httpx.ConnectError('offline')
        if failure_kind == 'timeout':
            raise httpx.ReadTimeout('timeout')
        return httpx.Response(503 if failure_kind == 'server' else 429)
    response[0] = fail
    worker.scan()
    for i in range(5):
        worker.flush()
        if i < 4:
            clock[0] += 4000
    assert worker.status()['counts'] == {'unavailable': 1}
    exhausted_at = clock[0]
    # Restart preserves both the exhausted budget and its cooldown.
    reopened = AvatarBackfill(worker.archive_dir, worker.directory, client=worker.client, clock=lambda: clock[0])
    reopened._contacts = worker._contacts
    try:
        response[0] = lambda request: httpx.Response(200, content=image_bytes())
        clock[0] = exhausted_at + 6 * 3600 - 1
        reopened.scan()
        reopened.flush()
        assert reopened.status()['counts'] == {'unavailable': 1}
        assert len(requests) == 5
        clock[0] += 1
        assert reopened.scan()['counts'] == {'pending': 1}
        assert reopened.flush()['counts'] == {'ready': 1}
        assert len(requests) == 6
    finally:
        reopened.close()


@pytest.mark.parametrize('status', [400, 401, 403, 404, 410])
def test_permanent_http_failure_does_not_retry_after_cooldown(setup, status):
    worker, _, _, _, clock, _, requests, response = setup
    response[0] = lambda request: httpx.Response(status)
    worker.scan()
    assert worker.flush()['counts'] == {'unavailable': 1}
    clock[0] += 7 * 3600
    worker.scan()
    worker.flush()
    assert len(requests) == 1
    assert worker.status()['counts'] == {'unavailable': 1}
