#!/usr/bin/python3
"""DA07 synthetic, durable Notion API; no remote requests or real credentials."""
import base64, hashlib, json, os, sqlite3, stat, sys, uuid
from pathlib import Path
root=Path(__file__).parent
import traceback
def record_exception(kind,value,tb):
 with (root/'errors').open('a') as out:traceback.print_exception(kind,value,tb,file=out)
 traceback.print_exception(kind,value,tb)
sys.excepthook=record_exception
args=sys.argv[1:]
with (root/'calls').open('a') as out:out.write(json.dumps(args)+'\n')
if args==['--version']:
 print('ntn 0.22.10');sys.exit(0)
assert args[0]=='api' and args[-2:]==['--notion-version','2026-03-11']
assert os.environ['NOTION_API_TOKEN']=='SYNTHETIC_NOTION_WORKER_TOKEN'
assert os.environ['NOTION_KEYRING']=='0' and os.environ['NOTION_API_BASE_URL']=='https://api.notion.com'
assert Path(os.environ['HOME']).resolve()==Path.cwd()
dbroot=Path((root/'db-root').read_text()) if (root/'db-root').exists() else root/'store'
db=sqlite3.connect(dbroot/'archive.sqlite',timeout=.5)
db.execute('BEGIN IMMEDIATE')
assert db.execute("SELECT count(*) FROM notion_queue WHERE state='creating'").fetchone()[0]==1
assert db.execute('SELECT count(*) FROM notion_attempts WHERE finished_at IS NULL').fetchone()[0]==1
db.rollback();db.close()
mode=(root/'mode').read_text();path=args[1];method=args[args.index('-X')+1]
body=None
if '-d' in args:
 f=Path(args[args.index('-d')+1].removeprefix('@'))
 assert f.is_relative_to(Path.cwd()) and stat.S_IMODE(f.stat().st_mode)==0o600
 body=json.loads(f.read_text())
statefile=root/'remote.json'
state=json.loads(statefile.read_text()) if statefile.exists() else {'pages':{},'uploads':{},'next_block':10000,'pdf_appends':0,'sends':0}
def save():
 f=root/'remote.new'
 with f.open('w') as out:json.dump(state,out);out.flush();os.fsync(out.fileno())
 os.replace(f,statefile)
def error(status,code='synthetic_error'):
 print(json.dumps({'object':'error','status':status,'code':code}));sys.exit(1)
def objectid(number):return str(uuid.UUID(int=number))
def blocks(children):
 result=[]
 for child in children:
  child=json.loads(json.dumps(child));child['id']=objectid(state['next_block']);state['next_block']+=1;child['has_children']=False
  for item in child.get(child['type'],{}).get('rich_text',[]):item['plain_text']=item['text']['content']
  result.append(child)
 return result
def summary(page):return {k:v for k,v in page.items() if k!='blocks'}
def returned(block):
 block=json.loads(json.dumps(block))
 if block['type']=='pdf':
  block['pdf']={'type':'file','file':{'url':'http://files.example.org/'+block['id']+'?signature='+str(len((root/'calls').read_text())),'expiry_time':'2030-01-01T00:00:00Z'},'caption':block['pdf'].get('caption',[])}
 return block
if path=='v1/pages' and method=='POST':
 page_id=objectid(len(state['pages'])+1)
 page={'object':'page','id':page_id,'parent':body['parent'],'properties':body['properties'],'archived':False,'in_trash':False,'blocks':blocks(body['children'])}
 state['pages'][page_id]=page;save();print(json.dumps(summary(page)))
elif path=='v1/search':
 pages=[summary(p) for p in state['pages'].values() if any(body['query'] in t['text']['content'] for v in p['properties'].values() for t in v.get('title',[]))]
 print(json.dumps({'object':'list','results':pages,'has_more':False,'next_cursor':None}))
elif path.startswith('v1/pages/'):
 print(json.dumps(summary(state['pages'][path.split('/')[2]])))
elif path=='v1/file_uploads':
 if mode=='unsupported':error(400,'unsupported_feature')
 if mode=='auth_failed':error(403,'restricted_resource')
 assert body['mode']=='single_part' and body['content_type']=='application/pdf' and body['filename'].endswith('.pdf')
 ident=objectid(1000+len(state['uploads']));upload={'object':'file_upload','id':ident,'status':'pending','expiry_time':'2030-01-01T00:00:00Z','content_length':None,'content_type':'application/pdf','filename':body['filename']}
 state['uploads'][ident]=upload;save();print(json.dumps(upload))
elif path.startswith('v1/file_uploads/'):
 ident=path.split('/')[2];upload=state['uploads'][ident]
 if path.endswith('/send'):
  assert method=='POST' and '--file' in args
  file=Path(args[args.index('--file')+1]);assert file.is_relative_to(Path.cwd())
  raw=file.read_bytes();assert hashlib.sha256(raw).hexdigest()==hashlib.sha256((root/'original.pdf').read_bytes()).hexdigest()
  if mode=='upload_failed':error(500)
  upload['status']='uploaded';upload['content_length']=len(raw);state['sends']+=1;save()
  if mode=='send_lost':print('response lost');sys.exit(0)
 if mode=='expired':upload['status']='expired';upload['expiry_time']='2000-01-01T00:00:00Z'
 print(json.dumps(upload))
elif path.endswith('/children'):
 page=state['pages'][path.split('/')[2]]
 if method=='PATCH':
  pdf=any(v['type']=='pdf' for v in body['children'])
  if pdf and mode in ['attachment_failed','expired']:error(429)
  children=blocks(body['children']);page['blocks'].extend(children)
  if pdf:state['pdf_appends']+=1
  save()
  if pdf and mode in ['append_lost','append_lost_hidden']:print('accepted but lost response')
  else:print(json.dumps({'object':'list','results':[returned(v) for v in children],'has_more':False,'next_cursor':None}))
 else:
  children=[returned(v) for v in page['blocks'] if not(mode=='append_lost_hidden' and v['type']=='pdf')]
  start=int(next((a.split('==',1)[1] for a in args if a.startswith('start_cursor==')),'0'));end=min(start+100,len(children))
  print(json.dumps({'object':'list','results':children[start:end],'has_more':end<len(children),'next_cursor':str(end) if end<len(children) else None}))
else:
 assert path.startswith('v1/blocks/')
 ident=path.split('/')[2];found=[block for page in state['pages'].values() for block in page['blocks'] if block['id']==ident]
 assert len(found)==1
 block=found[0]
 if mode=='missing_attachment' and block['type']=='pdf':error(404,'object_not_found')
 if method=='PATCH':
  assert block['type']=='paragraph' and 'paragraph' in body
  block['paragraph']=body['paragraph'];save()
 print(json.dumps(returned(block)))
