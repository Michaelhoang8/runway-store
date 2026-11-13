"""Bounded educational LSM store: CRC-framed WAL, immutable runs, atomic manifest."""
import bisect
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import threading
import time
import uuid
import zlib


class Conflict(ValueError): pass
class Corruption(ValueError): pass
class SimulatedCrash(RuntimeError): pass


def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode()
def checksum(data):return hashlib.sha256(data).hexdigest()
def valid_key(key):
    if not isinstance(key,str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9._/-]{0,63}',key):raise ValueError('Keys must be 1–64 ASCII letters, digits, dots, underscores, slashes, or hyphens, starting with a letter or digit.')
def valid_value(value):
    if value is not None and (not isinstance(value,str) or any(0xD800<=ord(c)<=0xDFFF for c in value) or len(value.encode('utf-8'))>4096):raise ValueError('Values must be UTF-8 text of at most 4,096 bytes, or null for a tombstone.')


class Store:
    def __init__(self,directory):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True)
        self.lock=threading.RLock();self.closed=False;self.lock_path=self.directory/'writer.lock'
        try:self.lock_fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY)
        except FileExistsError:raise Conflict('Workspace has a writer.lock. Stop its owner before removing a stale lock after a crash.')
        try:
            os.write(self.lock_fd,str(os.getpid()).encode());os.fsync(self.lock_fd)
            self.wal=self.directory/'wal.bin';self.manifest_path=self.directory/'manifest.json'
            if not self.manifest_path.exists():
                if self.wal.exists() or any(self.directory.glob('run-*.json')):raise Corruption('Manifest missing from an existing workspace.')
                self.atomic(self.manifest_path,encode({'format':1,'durableSequence':0,'generation':0,'runs':[]}))
            self.manifest=json.loads(self.manifest_path.read_bytes());self.validate_manifest()
            self.runs=[self.read_run(info) for info in self.manifest['runs']]
            self.mem={};self.sequence=self.manifest['durableSequence'];self.recovered=0;self.discarded_tail=0
            self.replay()
        except BaseException:self.close();raise

    def close(self):
        with self.lock:
            if not self.closed:
                self.closed=True;os.close(self.lock_fd);self.lock_path.unlink(missing_ok=True)

    def __enter__(self):return self
    def __exit__(self,*args):self.close()
    def check_open(self):
        if self.closed:raise ValueError('Store is closed.')

    def atomic(self,path,data):
        temporary=self.directory/('tmp-'+uuid.uuid4().hex)
        with temporary.open('xb') as file:file.write(data);file.flush();os.fsync(file.fileno())
        for attempt in range(5):
            try:os.replace(temporary,path);break
            except PermissionError:
                if attempt==4:raise
                time.sleep(.01*2**attempt)

    def validate_manifest(self):
        m=self.manifest
        if not isinstance(m,dict) or set(m)!={'format','durableSequence','generation','runs'} or m['format']!=1 or type(m['durableSequence']) is not int or not 0<=m['durableSequence']<=10000 or type(m['generation']) is not int or not 0<=m['generation']<=20000 or not isinstance(m['runs'],list) or len(m['runs'])>200:raise Corruption('Invalid manifest.')
        names=set()
        for r in m['runs']:
            if not isinstance(r,dict) or set(r)!={'file','sha256','count'} or not isinstance(r['file'],str) or not re.fullmatch(r'run-[0-9a-f]{32}\.json',r['file']) or r['file'] in names or not isinstance(r['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',r['sha256']) or type(r['count']) is not int or not 1<=r['count']<=1000:raise Corruption('Invalid run descriptor.')
            names.add(r['file'])

    def validate_record(self,r):
        if not isinstance(r,dict) or set(r)!={'key','value','sequence'} or type(r['sequence']) is not int or not 1<=r['sequence']<=10000:raise Corruption('Invalid record.')
        try:valid_key(r['key']);valid_value(r['value'])
        except ValueError as error:raise Corruption(str(error)) from error

    def read_run(self,info):
        path=self.directory/info['file']
        if not path.is_file() or path.stat().st_size>25000000:raise Corruption('Missing or oversized run file.')
        data=path.read_bytes()
        if checksum(data)!=info['sha256']:raise Corruption('Run checksum mismatch: '+info['file'])
        rows=json.loads(data)
        if not isinstance(rows,list) or len(rows)!=info['count']:raise Corruption('Run count mismatch.')
        previous=None
        for row in rows:
            self.validate_record(row)
            if row['sequence']>self.manifest['durableSequence'] or previous is not None and row['key']<=previous:raise Corruption('Run ordering or sequence mismatch.')
            previous=row['key']
        return rows

    def replay(self):
        if not self.wal.exists():self.atomic(self.wal,b'')
        if self.wal.stat().st_size>250000000:raise Corruption('WAL exceeds demo bounds.')
        data=self.wal.read_bytes();offset=0;previous=None
        while offset<len(data):
            start=offset
            if len(data)-offset<12:self.discarded_tail=len(data)-start;break
            length,inverse,crc=struct.unpack('>III',data[offset:offset+12]);offset+=12
            if inverse!=(length^0xffffffff):raise Corruption('WAL frame header mismatch.')
            if not 1<=length<=25000:raise Corruption('Invalid WAL frame length.')
            if len(data)-offset<length:self.discarded_tail=len(data)-start;break
            payload=data[offset:offset+length];offset+=length
            if zlib.crc32(payload)!=crc:raise Corruption('WAL checksum mismatch; refusing silent data loss.')
            row=json.loads(payload);self.validate_record(row)
            if previous is not None and row['sequence']!=previous+1:raise Corruption('Non-contiguous WAL sequence.')
            previous=row['sequence']
            if row['sequence']<=self.manifest['durableSequence']:continue
            if row['sequence']!=self.sequence+1:raise Corruption('WAL does not continue the durable sequence.')
            self.mem[row['key']]=row;self.sequence=row['sequence'];self.recovered+=1
        if self.discarded_tail:
            with self.wal.open('r+b') as file:file.truncate(len(data)-self.discarded_tail);file.flush();os.fsync(file.fileno())
        if len(self.latest())>1000:raise Corruption('Too many retained keys.')

    def latest(self):
        merged={}
        for run in self.runs:
            for row in run:
                if row['sequence']>merged.get(row['key'],{}).get('sequence',0):merged[row['key']]=row
        merged.update(self.mem)
        return merged

    def put(self,key,value,expected_sequence):
        valid_key(key);valid_value(value)
        with self.lock:
            self.check_open()
            if type(expected_sequence) is not int or expected_sequence!=self.sequence:raise Conflict('Store sequence changed. Refresh before writing.')
            if self.sequence>=10000:raise ValueError('This workspace is limited to 10,000 writes.')
            if key not in self.latest() and len(self.latest())>=1000:raise ValueError('This workspace is limited to 1,000 retained keys, including tombstones.')
            row={'key':key,'value':value,'sequence':self.sequence+1};payload=encode(row);frame=struct.pack('>III',len(payload),len(payload)^0xffffffff,zlib.crc32(payload))+payload
            try:
                with self.wal.open('ab') as file:file.write(frame);file.flush();os.fsync(file.fileno())
            except OSError:
                self.close();raise
            self.mem[key]=row;self.sequence+=1
            return dict(row)

    def lookup(self,key):
        valid_key(key)
        with self.lock:
            self.check_open();probes=[]
            if key in self.mem:return {'record':dict(self.mem[key]),'source':'memtable','probes':['memtable']}
            probes.append('memtable');found=None;source=None
            for info,run in reversed(list(zip(self.manifest['runs'],self.runs))):
                probes.append(info['file']);keys=[r['key'] for r in run];index=bisect.bisect_left(keys,key)
                if index<len(run) and run[index]['key']==key:
                    row=run[index]
                    if found is None or row['sequence']>found['sequence']:found=row;source=info['file']
            return {'record':dict(found) if found else None,'source':source,'probes':probes}

    def write_run(self,rows):
        data=encode(rows);name='run-'+uuid.uuid4().hex+'.json';self.atomic(self.directory/name,data)
        return {'file':name,'sha256':checksum(data),'count':len(rows)}

    def publish(self,manifest):
        self.atomic(self.manifest_path,encode(manifest));self.manifest=manifest

    def flush(self,fault=None):
        with self.lock:
            self.check_open()
            if not self.mem:return {'changed':False}
            if len(self.runs)>=200:raise ValueError('Compact before adding another run.')
            rows=sorted(self.mem.values(),key=lambda r:r['key']);info=self.write_run(rows)
            if fault=='after_run':raise SimulatedCrash('Run written before manifest publication.')
            self.publish({**self.manifest,'generation':self.manifest['generation']+1,'durableSequence':self.sequence,'runs':self.manifest['runs']+[info]})
            self.runs.append(rows);self.mem={}
            if fault=='after_manifest':raise SimulatedCrash('Manifest published before WAL reset.')
            self.atomic(self.wal,b'')
            return {'changed':True,'run':info}

    def compact(self,fault=None):
        with self.lock:
            self.check_open();self.flush()
            if len(self.runs)<2:return {'changed':False}
            rows=sorted(self.latest().values(),key=lambda r:r['key']);info=self.write_run(rows)
            if fault=='after_run':raise SimulatedCrash('Compacted run written before manifest publication.')
            before=sum(len(run) for run in self.runs)
            self.publish({**self.manifest,'generation':self.manifest['generation']+1,'runs':[info]});self.runs=[rows]
            return {'changed':True,'beforeRecords':before,'afterRecords':len(rows),'run':info}

    def snapshot(self,prefix=''):
        if not isinstance(prefix,str) or len(prefix)>64:raise ValueError('Prefix must be at most 64 characters.')
        with self.lock:
            self.check_open();latest=self.latest();active={r['file'] for r in self.manifest['runs']}
            orphans=[p.name for p in self.directory.glob('run-*.json') if p.name not in active]
            return {'format':'runway-store-evidence-v1','sequence':self.sequence,'durableSequence':self.manifest['durableSequence'],'generation':self.manifest['generation'],'prefix':prefix,'records':[dict(latest[k]) for k in sorted(latest) if k.startswith(prefix)],'memtable':[dict(self.mem[k]) for k in sorted(self.mem)],'runs':[dict(info) for info in self.manifest['runs']],'walBytes':self.wal.stat().st_size,'recoveredWrites':self.recovered,'discardedTailBytes':self.discarded_tail,'orphanFiles':sorted(orphans),'totalRetainedKeys':len(latest)}
