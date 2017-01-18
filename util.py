# utilities of project

from tabulate import tabulate
from progressbar import ProgressBar, Percentage, Bar, Timer
from threading import Thread

import multiprocessing as mp
import json 
import time

__all__ = ["varType", "entryReader", "parmap", "show", "transpose"]

POOL_SIZE = mp.cpu_count()

def varType(d):
	typeList = [int, float, str, dict, list]
	for t in typeList:
		if isinstance(d, t):
			return t.__name__
	return None

def entryReader(string):
	try:
		return json.loads( string )
	except:
		return json.loads( string.replace("'",'"') )

def spawn(f, qin, qout):
	while True:
		x = qin.get()
		if x is None:
			break
		qout.put_nowait( f(x) )

def parmap(target, inputs, probar=True):

	pbar = ProgressBar(widgets=[Percentage(), Bar(), Timer()], max_value=len(inputs)).start()
	isfinished = False
	def updating():
		while not(isfinished):
			pbar.update()
			time.sleep(1)
		pbar.finish()
	Thread(target=updating).start()


	qin = mp.Queue(1)
	qout = mp.Queue()
	proc = [ mp.Process(target=spawn, args=(target, qin, qout)) for i in range(POOL_SIZE) ]
	
	for p in  proc:
		p.daemon = True
		p.start()

	[ qin.put(x) for x in inputs ]
	[ qin.put(None) for _ in range(POOL_SIZE) ]
	
	ret = []
	for i in range(len(inputs)):
		ret.append( qout.get() )
		pbar.update(i+1)

	[ p.join() for p in proc ]

	isfinished = True

	return ret

def show(data, limit = False, truncate = False, toPrint = True):
	if limit == False:
		data = list(data)
	else:
		limited = []
		i = 0
		for d in data:
			limited.append(d)
			i += 1
			if i >= limit:
				break
		data = limited

	if len(data) == 0:
		print("Nothing returns")
		return None

	keys = list( data[0].keys() )
	if "Key" in keys:
		keys = [k for k in keys if k != "Key"]
		keys.append("Key")
		keys.reverse()

	p = tabulate([ [ str(d[k])[:20]+"..." if truncate==True and len(str(d[k]))>20 else d[k] for k in keys ] for d in data ], keys)
	if toPrint == True:
		print( p )
	# return p

def transpose(data):
	trans = {}
	for d in data:
		for k in d:
			if not(k in trans):
				trans[k] = []
			trans[k].append( d[k] )

	return iter([ { **{"Key": k}, **dict(zip(range(len(trans[k])), trans[k])) } for k in trans ])
