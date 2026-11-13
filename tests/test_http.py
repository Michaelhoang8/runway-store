import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from run import Handler
from store import Store


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.store=Store(self.temp.name)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join();self.server.store.close();self.temp.cleanup()

    def request(self,method,path,body=None,headers=None):
        client=HTTPConnection(*self.server.server_address,timeout=3)
        client.request(method,path,None if body is None else json.dumps(body),headers or {'Content-Type':'application/json'})
        response=client.getresponse();raw=response.read();status=response.status
        self.assertIn("frame-ancestors 'none'",response.headers['Content-Security-Policy'])
        mime=response.headers['Content-Type'];client.close()
        return status,json.loads(raw) if raw and mime.startswith('application/json') else raw

    def test_write_flush_lookup_and_compaction(self):
        self.assertEqual(self.request('POST','/api/records',{'key':'a','value':'one','expectedSequence':0})[0],201)
        self.assertEqual(self.request('POST','/api/records',{'key':'b','value':'stale','expectedSequence':0})[0],409)
        self.assertTrue(self.request('POST','/api/flush',{})[1]['changed'])
        self.assertEqual(self.request('POST','/api/records',{'key':'a','value':None,'expectedSequence':1})[0],201)
        self.assertTrue(self.request('POST','/api/compact',{})[1]['changed'])
        state=self.request('GET','/api/state')[1]
        self.assertEqual(state['sequence'],2);self.assertEqual(len(state['runs']),1)
        self.assertEqual(self.request('GET','/api/lookup?key=a')[1]['record']['value'],None)
        self.assertEqual(self.request('GET','/api/state?prefix=b')[1]['records'],[])
        self.assertEqual(self.request('POST','/api/records',{})[0],400)
        self.assertEqual(self.request('POST','/api/flush',{'unexpected':1})[0],400)

    def test_boundaries(self):
        for path in ['/store.py','/data/wal.bin','/.git/config','/../run.py']:
            self.assertEqual(self.request('GET',path)[0],404)
        self.assertEqual(self.request('HEAD','/')[1],b'')
        self.assertEqual(self.request('GET','/',headers={'Host':'external.invalid'})[0],400)
        self.assertEqual(self.request('POST','/api/records',{}, {'Content-Type':'application/json','Origin':'https://other.invalid'})[0],403)
        self.assertEqual(self.request('POST','/api/records',{}, {'Content-Type':'text/plain'})[0],415)
        self.assertEqual(self.request('POST','/api/records',{'x':'a'*65536})[0],413)
        self.assertEqual(self.request('PUT','/api/records',{})[0],405)
