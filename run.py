"""Serve Runway Store locally with a persistent WAL and sorted-run workbench."""
import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from store import Store, Conflict

ROOT = Path(__file__).resolve().parent
ASSETS = {'/': ('index.html','text/html'), '/app.js': ('app.js','text/javascript'), '/style.css': ('style.css','text/css')}


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, value, mime='application/json'):
        body = value if isinstance(value, bytes) else json.dumps(value, allow_nan=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', mime+'; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    def handle_request(self):
        try:
            hosts = self.headers.get_all('Host', [])
            if len(hosts) != 1 or not re.fullmatch(r'(localhost|127\.0\.0\.1)(:[0-9]{1,5})?', hosts[0], re.I):
                return self.reply(400, {'error': 'Use a loopback Host header.'})
            path = urlsplit(self.path).path
            query=parse_qs(urlsplit(self.path).query)
            if self.command in ('GET','HEAD'):
                if path in ASSETS:
                    filename,mime=ASSETS[path]
                    return self.reply(200,(ROOT/filename).read_bytes(),mime)
                if path == '/api/state':
                    return self.reply(200,self.server.store.snapshot(query.get('prefix',[''])[0]))
                if path == '/api/lookup':
                    return self.reply(200,self.server.store.lookup(query.get('key',[''])[0]))
                return self.reply(404,{'error':'Not found.'})
            if self.command != 'POST':
                return self.reply(405,{'error':'Method not supported.'})
            origin=self.headers.get('Origin')
            if origin is not None and origin not in (f'http://localhost:{self.server.server_port}',f'http://127.0.0.1:{self.server.server_port}'):
                return self.reply(403,{'error':'Cross-origin requests are rejected.'})
            lengths=self.headers.get_all('Content-Length',[])
            if self.headers.get('Transfer-Encoding') or len(lengths)!=1 or not lengths[0].isdigit() or not 0<int(lengths[0])<=16384:
                return self.reply(413,{'error':'Provide a JSON body of at most 16 KB.'})
            if self.headers.get('Content-Type','').split(';')[0].strip()!='application/json':
                return self.reply(415,{'error':'Use application/json.'})
            body=json.loads(self.rfile.read(int(lengths[0])).decode('utf-8'))
            if path=='/api/records':
                if not isinstance(body,dict) or set(body)!={'key','value','expectedSequence'}:raise ValueError('Provide key, value, and expectedSequence.')
                return self.reply(201,self.server.store.put(body['key'],body['value'],body['expectedSequence']))
            if path in ('/api/flush','/api/compact'):
                if body!={}:raise ValueError('Provide an empty JSON object.')
                return self.reply(200,self.server.store.flush() if path=='/api/flush' else self.server.store.compact())
            return self.reply(404,{'error':'Not found.'})
        except Conflict as error:
            self.reply(409,{'error':str(error)})
        except (ValueError,UnicodeDecodeError,RecursionError) as error:
            self.reply(400,{'error':str(error)})
        except OSError:
            self.reply(503,{'error':'Storage operation failed. Stop and reopen the workspace before retrying.'})

    do_GET=handle_request
    do_HEAD=handle_request
    do_POST=handle_request
    do_PUT=handle_request
    do_DELETE=handle_request


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,default=8000)
    parser.add_argument('--data',type=Path,default=ROOT/'data')
    args=parser.parse_args()
    server=ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
    server.store=Store(args.data)
    print(f'Runway Store: http://127.0.0.1:{server.server_port}',flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        server.store.close()


if __name__=='__main__':
    main()
