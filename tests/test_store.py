import concurrent.futures
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest
from store import Store,Conflict,Corruption,SimulatedCrash


class StoreTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name);self.store=Store(self.path)
    def tearDown(self):self.store.close();self.temp.cleanup()
    def reopen(self):self.store.close();self.store=Store(self.path);return self.store
    def test_wal_recovery_and_tombstones(self):
        self.store.put('a','one',0);self.store.put('b','two',1);self.store.put('a',None,2)
        s=self.reopen().snapshot();self.assertEqual(s['sequence'],3);self.assertEqual(s['recoveredWrites'],3)
        self.assertIsNone(self.store.lookup('a')['record']['value']);self.assertEqual(self.store.lookup('b')['record']['value'],'two')
        self.store.flush();s=self.reopen().snapshot();self.assertEqual(s['durableSequence'],3);self.assertEqual(s['walBytes'],0);self.assertEqual(s['memtable'],[])

    def test_flush_crash_boundaries(self):
        self.store.put('a','one',0)
        with self.assertRaises(SimulatedCrash):self.store.flush('after_run')
        s=self.reopen().snapshot();self.assertEqual(s['sequence'],1);self.assertEqual(s['durableSequence'],0);self.assertEqual(len(s['orphanFiles']),1)
        with self.assertRaises(SimulatedCrash):self.store.flush('after_manifest')
        s=self.reopen().snapshot();self.assertEqual(s['sequence'],1);self.assertEqual(s['durableSequence'],1);self.assertEqual(s['memtable'],[])
        self.store.put('a','two',1);self.assertEqual(self.reopen().lookup('a')['record']['value'],'two')

    def test_compaction_preserves_latest_values_and_tombstones(self):
        self.store.put('a','old',0);self.store.put('b','keep',1);self.store.flush()
        self.store.put('a',None,2);self.store.put('c','new',3);self.store.flush()
        before=self.store.snapshot()['records']
        with self.assertRaises(SimulatedCrash):self.store.compact('after_run')
        self.assertEqual(self.reopen().snapshot()['records'],before)
        result=self.store.compact();self.assertEqual(result['beforeRecords'],4);self.assertEqual(result['afterRecords'],3)
        self.assertEqual(self.reopen().snapshot()['records'],before);self.assertEqual(len(self.store.snapshot()['runs']),1)
        self.assertIsNone(self.store.lookup('a')['record']['value']);self.assertEqual(self.store.lookup('missing')['record'],None)

    def test_every_partial_tail_cut_recovers_only_complete_frames(self):
        self.store.put('a','one',0);first=self.store.wal.read_bytes();self.store.put('b','two',1);full=self.store.wal.read_bytes();second=full[len(first):]
        self.store.close()
        for cut in range(1,len(second)):
            self.path.joinpath('wal.bin').write_bytes(first+second[:cut]);self.store=Store(self.path)
            self.assertEqual(self.store.sequence,1);self.assertEqual(self.store.discarded_tail,cut);self.assertEqual(self.store.wal.read_bytes(),first);self.store.close()
        self.path.joinpath('wal.bin').write_bytes(full);self.store=Store(self.path);self.assertEqual(self.store.sequence,2)

    def test_corruption_is_not_silently_skipped(self):
        self.store.put('a','one',0);original=self.store.wal.read_bytes();self.store.close()
        for position in [0,4,8,13,len(original)-1]:
            altered=bytearray(original);altered[position]^=1;self.path.joinpath('wal.bin').write_bytes(altered)
            with self.assertRaises(Corruption):Store(self.path)
            self.assertEqual(self.path.joinpath('wal.bin').read_bytes(),altered)
        self.path.joinpath('wal.bin').write_bytes(original);self.store=Store(self.path);self.store.flush();run=self.path/self.store.manifest['runs'][0]['file'];data=run.read_bytes();self.store.close();run.write_bytes(data+b' ')
        with self.assertRaises(Corruption):Store(self.path)

    def test_concurrent_cas_has_one_winner_and_exclusive_writer_lock(self):
        with self.assertRaises(Conflict):Store(self.path)
        def write(i):
            try:self.store.put('key',str(i),0);return True
            except Conflict:return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:self.assertEqual(sum(pool.map(write,range(20))),1)
        self.assertEqual(self.reopen().sequence,1)

    def test_random_operations_flush_compaction_and_restarts_match_dictionary(self):
        rng=random.Random(73);expected={}
        for i in range(200):
            key='key/'+str(rng.randrange(20));value=None if rng.random()<.3 else str(rng.randrange(1000));self.store.put(key,value,i);expected[key]={'key':key,'value':value,'sequence':i+1}
            if i%7==0:self.store.flush()
            if i%23==0:self.store.compact()
            if i%11==0:self.reopen()
            self.assertEqual(self.store.snapshot()['records'],[expected[k] for k in sorted(expected)])
            for k in expected:self.assertEqual(self.store.lookup(k)['record'],expected[k])
        self.assertEqual([r['key'] for r in self.store.snapshot('key/1')['records']],[k for k in sorted(expected) if k.startswith('key/1')])

    def test_bounds_closed_store_and_missing_manifest(self):
        for key,value in [('../invalid','x'),('bad key','x'),('valid',True),('valid','x'*4097),('valid','\ud800')]:
            with self.assertRaises(ValueError):self.store.put(key,value,0)
        with self.assertRaises(Conflict):self.store.put('a','x',True)
        self.store.close()
        with self.assertRaises(ValueError):self.store.put('a','x',0)
        self.path.joinpath('manifest.json').unlink()
        with self.assertRaises(Corruption):Store(self.path)

    def test_abrupt_subprocess_exit_leaves_lock_and_recovers_committed_data(self):
        source="""
import os,sys
from store import Store,SimulatedCrash
s=Store(sys.argv[1]);s.put('a','durable',0);s.put('b',None,1)
try:
 if sys.argv[2]!='wal':s.flush(sys.argv[2])
except SimulatedCrash:pass
os._exit(17)
"""
        for phase in ['wal','after_run','after_manifest']:
            directory=self.path/phase
            process=subprocess.run([sys.executable,'-c',source,str(directory),phase],cwd=Path(__file__).resolve().parents[1],capture_output=True)
            self.assertEqual(process.returncode,17,process.stderr)
            with self.assertRaises(Conflict):Store(directory)
            # This child is confirmed exited; only its disposable test lock is removed.
            (directory/'writer.lock').unlink()
            with Store(directory) as recovered:
                self.assertEqual(recovered.sequence,2)
                self.assertEqual(recovered.lookup('a')['record']['value'],'durable')
                self.assertIsNone(recovered.lookup('b')['record']['value'])
