#!/usr/bin/python3
"""DA08 strict, stateful, offline gh fixture. Never imports a network client."""
import json, os, pathlib, stat, subprocess, sys, time
root = pathlib.Path(__file__).resolve().parent
assert os.environ['GH_TOKEN'] == 'SYNTHETIC_GH_DA08_TOKEN'
assert os.environ['GH_HOST'] == 'github.com'
assert os.environ['GH_PROMPT_DISABLED'] == '1'
assert 'DA08_PRIVATE_SEED' not in os.environ
assert pathlib.Path.cwd() == pathlib.Path(os.environ['HOME'])
assert pathlib.Path(os.environ['GH_CONFIG_DIR']).parent == pathlib.Path.cwd()
argv = sys.argv[1:]
assert argv[:1] == ['issue'] and argv[1] in ['list', 'view', 'create', 'comment', 'reopen']
action = argv[1]
options, positional = {}, []
i = 2
while i < len(argv):
    if argv[i].startswith('--'):
        assert i + 1 < len(argv)
        options.setdefault(argv[i], []).append(argv[i+1]); i += 2
    else:
        positional.append(argv[i]); i += 1
def one(name):
    assert len(options[name]) == 1
    return options[name][0]
repo = one('--repo')
assert repo == 'RyderFreeman4Logos/telegram-archive-bot'
allowed = {
    'list': {'--repo', '--state', '--search', '--limit', '--json'},
    'view': {'--repo', '--json'},
    'create': {'--repo', '--title', '--body-file', '--label'},
    'comment': {'--repo', '--body-file'},
    'reopen': {'--repo'},
}
assert set(options) <= allowed[action]
assert len(positional) == (0 if action in ['list', 'create'] else 1)
if action == 'list':
    assert one('--state') == 'all' and one('--limit') == '100'
    assert one('--json') == 'number,url,state,body'
    assert one('--search').startswith('"da08-fingerprint:') and one('--search').endswith('"')
if action == 'view': assert one('--json') == 'number,url,state,body,comments'
with (root/'calls.jsonl').open('a') as f: f.write(json.dumps({'action': action, 'repository': repo})+'\n')
mode = (root/'mode').read_text() if (root/'mode').exists() else 'normal'
if mode == 'auth':
    print('SYNTHETIC_GH_DA08_TOKEN DA08_PRIVATE_SEED',file=sys.stderr);sys.exit(4)
if mode == 'temporary': sys.exit(1)
path = root/'remote.json'
remote = json.loads(path.read_text()) if path.exists() else {'issues': []}
def save():
    pending=path.with_suffix('.new');pending.write_text(json.dumps(remote));pending.replace(path)
def body():
    p=pathlib.Path(one('--body-file'))
    assert p.parent == pathlib.Path.cwd()
    assert stat.S_IMODE(p.stat().st_mode) == 0o600
    text=p.read_text();assert len(text.encode()) <= 32768
    assert 'SYNTHETIC_GH_DA08_TOKEN' not in text
    with (root/'public-bodies.jsonl').open('a') as f:f.write(json.dumps(text)+'\n')
    return text
def hold():
    child=subprocess.Popen(['/usr/bin/python3','-c','import time; time.sleep(120)'])
    (root/'accepted').write_text(str(child.pid))
    time.sleep(120)
if action == 'list':
    marker=one('--search').strip('"')
    values=[x for x in remote['issues'] if marker in x['body']]
    print(json.dumps([] if mode == 'hidden' else values))
elif action == 'create':
    n=len(remote['issues'])+1
    issue={'number': n,'url': f'https://github.com/{repo}/issues/{n}', 'state':'OPEN','body': body(),'comments': []}
    remote['issues'].append(issue);save()
    if mode == 'hold_create':hold()
    print(issue['url'])
else:
    issue=next(x for x in remote['issues'] if x['number'] == int(positional[0]))
    if action == 'view':
        value=dict(issue)
        if mode == 'wrong_body':value['body'] += '\nREMOTE EDIT'
        print(json.dumps(value))
    elif action == 'comment':
        issue['comments'].append({'body':body()});save()
        if mode == 'hold_comment':hold()
        print(issue['url']+'#issuecomment-1')
    elif action == 'reopen':
        issue['state']='OPEN';save();print('Reopened')
