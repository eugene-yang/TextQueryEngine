# utilities of project

from tabulate import tabulate
from progressbar import ProgressBar, Percentage, Bar, Timer
from threading import Thread
from random import random
from copy import deepcopy
import os

import multiprocessing as mp
import json 
import time

__all__ = ["configGetter", "varType", "entryReader", "parmap", "show", "transpose"]

POOL_SIZE = mp.cpu_count()

__config__ = None
def configGetter(key):
	global __config__
	if __config__ == None:
		__config__ = json.load( open("./config.json") )
	return __config__[key]

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

def spawn(f, qin, qout, qcount, tmpEnable, i):
	import pickle
	tempfnbase = configGetter("temp_dir")+ str( int(random()*1000000) ) + "-" + str(i) + "-"
	c = 0
	
	while True:
		x = qin.get()
		if x == None:
			break
		
		if tmpEnable:
			with open( tempfnbase + str(c), "wb" ) as fw:
				pickle.dump( f(x), fw )
				fw.close()
			qout.put_nowait( tempfnbase + str(c) )
		else:
			qout.put_nowait( f(x) )
		qcount.put_nowait(1)
		c += 1

def parmap(target, inputs, probar=True, tmpEnable=False):
	pbar = ProgressBar(widgets=[Percentage(), Bar(), Timer()], max_value=len(inputs)).start()
	pbar.update(0)
	isfinished = False
	
	qin = mp.Queue(1)
	qout = mp.JoinableQueue()
	qcount = mp.Queue()
	proc = [ mp.Process(target=spawn, args=(deepcopy(target), qin, qout, qcount, tmpEnable, i)) for i in range(POOL_SIZE) ]
	
	def updating():
		counter = 0
		while not( isfinished or counter == len(inputs) ):
			if not(qcount.empty()):
				qcount.get()
				counter += 1
			pbar.update(counter)
			time.sleep(1)
	Thread(target=updating).start()

	for p in proc:
		p.daemon = True
		p.start()

	[ qin.put(x) for x in inputs ]
	[ qin.put(None) for _ in range(POOL_SIZE) ]
		
	if tmpEnable == False:
		def ret():
			for i in range(len(inputs)):
				yield qout.get()
			qout.close()
			[ p.join() for p in proc ]
	else:
		fns = []
		for i in range(len(inputs)):
			fns.append( qout.get() )
		qout.close()
		[ p.join() for p in proc ]
		def ret():
			for fn in fns:
				yield pickle.load( open(fn, "rb") )
				os.remove( fn )

	pbar.finish()
	qin.close()

	isfinished = True

	return ret()

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
