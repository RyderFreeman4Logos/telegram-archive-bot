import hashlib
import http.server
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading

binary, root = Path(sys.argv[1]), Path(sys.argv[2])
os.umask(0o077)
calls = []
cancel_entered = threading.Event()
cancel_release = threading.Event()

class Model(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        assert self.path == '/v1/chat/completions'
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        assert request['stream'] is True
        content = request['messages'][-1]['content']
        if isinstance(content, list):
            content = ''.join(p.get('text', '') for p in content)
        source = json.loads(content.splitlines()[-1])
        calls.append(source['text'])
        if 'Cancellation witness' in source['text']:
            cancel_entered.set()
            cancel_release.wait(10)
        result = {'schema_version': 1, 'title': 'Fixture title',
                  'summary': 'Only the supplied fixture content was summarized.',
                  'language': 'en', 'tags': [],
                  'source_blocks': [b['id'] for b in source['blocks']]}
        chunks = [
            {'id': 'fixture', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'fixture',
             'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': json.dumps(result)}, 'finish_reason': None}]},
            {'id': 'fixture', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'fixture',
             'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]},
        ]
        body = (''.join('data: ' + json.dumps(v) + '\n\n' for v in chunks) + 'data: [DONE]\n\n').encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Model)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
config = root / 'config.toml'
config.write_text(f'''state_dir = "{root}/state"
[telegram]
bot_id = 1
allowed_chats = [2]
allowed_users = [3]
[model]
enabled = true
endpoint = "http://127.0.0.1:{server.server_port}/v1"
name = "fixture"
deployment_revision = "operator-fixture-v1"
[limits]
llm_seconds = 10
job_seconds = 60
[archive]
directory = "{root}/archive"
push = false
''')

def invoke(args, data=None, timeout=180):
    return subprocess.run([str(binary), '--config', str(config), *args],
                          input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)

def counts(db):
    return {name: db.execute('SELECT count(*) FROM "' + name + '"').fetchone()[0]
            for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}

try:
    initialized = invoke(['init'])
    assert initialized.returncode == 0, initialized.stderr.decode()
    db = sqlite3.connect(root / 'state/archive.sqlite')
    snapshot = json.dumps({'state_dir': str(root / 'state'), 'telegram': {'bot_id': 1}})
    config_id = hashlib.sha256((snapshot + '\n{}').encode()).hexdigest()
    request = json.dumps({'kind': 'text', 'text': 'Unrelated live inbox source'})
    old_job = hashlib.sha256(b'1:77:0').hexdigest()
    db.execute('INSERT INTO config_snapshots VALUES(?,?,?)', (config_id, snapshot, '{}'))
    db.execute('INSERT INTO inbox VALUES(1,77,2,3,?)', ('[' + request + ']',))
    db.execute("INSERT INTO jobs(id,bot_id,update_id,ordinal,request,config_id,state) VALUES(?,1,77,0,?,?,'queued')", (old_job, request, config_id))
    db.execute('INSERT INTO polling_cursors VALUES(1,78)')
    db.commit()
    inputs = [root / 'first article.md', root / ('界' * 81 + '\narticle.md')]
    for index, path in enumerate(inputs):
        path.write_text(f'# Original {index}\n\nA distinct synthetic observation numbered {index}.\n')
    original = {path: path.read_bytes() for path in inputs}
    data = b'\0'.join(os.fsencode(p) for p in inputs) + b'\0'
    args = ['summary', '--stdin0', '--rename', '--auto-commit']
    first = invoke(args, data)
    assert first.returncode == 0, 'summary CLI must process read-only input: ' + first.stderr.decode()
    result = json.loads(first.stdout)
    assert result['complete'] == 2 and len(result['results']) == 2, result
    assert len(calls) == 2
    for item, path in zip(result['results'], inputs):
        files = [Path(p) for p in item['files']]
        assert len(files) == 2 and all(p.parent.name == item['job_id'] for p in files)
        assert any(p.read_bytes() == original[path] for p in files)
        assert any(b'Only the supplied fixture content' in p.read_bytes() for p in files)
        assert all('Fixture title' in p.name for p in files)
    assert db.execute('SELECT state FROM jobs WHERE id=?', (old_job,)).fetchone() == ('queued',)
    assert db.execute('SELECT * FROM polling_cursors').fetchall() == [(1, 78)]
    assert db.execute('SELECT count(*) FROM inbox WHERE update_id<0 AND bot_id=0').fetchone()[0] == 2
    assert db.execute('SELECT count(*) FROM delivery_attempts').fetchone()[0] == 0
    before = counts(db)
    repeated = invoke(args, data)
    assert repeated.returncode == 0 and json.loads(repeated.stdout) == result
    assert len(calls) == 2 and counts(db) == before
    job_id = result['results'][0]['job_id']
    db.execute("UPDATE jobs SET state='running',phase='archive',lease_until=0,generation=generation+1 WHERE id=?", (job_id,))
    db.commit()
    resumed = invoke(args, data)
    assert resumed.returncode == 0 and json.loads(resumed.stdout) == result
    assert len(calls) == 2 and counts(db) == before
    assert all(path.read_bytes() == content for path, content in original.items())
    assert (root / 'archive/.git').is_dir()
    assert subprocess.run(['git', '-C', str(root / 'archive'), 'fsck', '--strict'], stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode == 0
    plain = invoke(['summary', str(inputs[1])])
    assert plain.returncode == 0, plain.stderr.decode()
    assert all(Path(p).suffix == '.md' for p in json.loads(plain.stdout)['results'][0]['files'])
    missing = invoke(['summary', str(inputs[0]), str(root / 'missing.md')])
    assert missing.returncode != 0 and json.loads(missing.stdout)['failed'] == 1
    saved = Path(result['results'][0]['files'][0])
    saved.write_bytes(b'User-edited result must not be overwritten.')
    conflict = invoke(args, data)
    assert conflict.returncode != 0 and saved.read_bytes() == b'User-edited result must not be overwritten.'
    symlink = root / 'alias.md'
    symlink.symlink_to(inputs[0])
    assert invoke(['summary', str(symlink)], timeout=5).returncode != 0
    fifo = root / 'pipe.md'
    os.mkfifo(fifo)
    assert invoke(['summary', str(fifo)], timeout=5).returncode != 0
    assert invoke(['--url', 'http://127.0.0.1/private'], timeout=5).returncode != 0
    assert db.execute('SELECT * FROM polling_cursors').fetchall() == [(1, 78)]
    cancelling = [root / 'cancel.md', root / 'never-admitted.md']
    cancelling[0].write_text('Cancellation witness in a synthetic article.\n')
    cancelling[1].write_text('This later file must not be read or admitted.\n')
    inbox_before = db.execute('SELECT count(*) FROM inbox').fetchone()[0]
    child = subprocess.Popen([str(binary), '--config', str(config), 'summary', *map(str, cancelling)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert cancel_entered.wait(5), 'model cancellation boundary was not reached'
        child.terminate()
        stdout, stderr = child.communicate(timeout=10)
        cancelled = json.loads(stdout)
        assert child.returncode != 0 and cancelled['results'][1]['job_id'] is None
        assert db.execute('SELECT count(*) FROM inbox').fetchone()[0] == inbox_before + 1
        unread_digest = hashlib.sha256(cancelling[1].read_bytes()).hexdigest()
        assert db.execute('SELECT count(*) FROM blobs WHERE digest=?', (unread_digest,)).fetchone()[0] == 0
    finally:
        cancel_release.set()
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    print(json.dumps({'status': 'PASS', 'processed': 2, 'provider_calls_before_repeat': 2,
                      'input_files_unchanged': True, 'unrelated_job_untouched': True,
                      'polling_cursor_unchanged': True, 'expired_archive_lease_resumed': True,
                      'telegram_messages_sent': 0}))
finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)
