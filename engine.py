# query metheods

from util import *
from os import listdir, fstat
import json
import multiprocessing as mp

__all__ = ["tables", "Scan", "TableScan"]

__base_dir__ = "../"

# engine init
def init():
	data = {}
	metaList = [ t for t in listdir("./metadata") if t.split(".")[-1]=="meta"]
	for fn in metaList:
		tn = fn.split('.')[0]
		data[ tn ] = json.load( open("./metadata/" + fn) )
		data[ tn ]["dir_prefix"] = __base_dir__ + data[tn]["dir"] + "/"
	return data

__meta__ = init()
def tables():
	return list(__meta__.keys())



class Scan():
	def __init__(self, obj):
		if varType(obj) == "str":
			# read table
			obj = TableScan(obj)
			

		if isinstance(obj, list):
			# try ordinary list of dict
			self.target = iter(obj)
			self.cols = obj[0].keys()
		elif isinstance(obj, TableScan):
			# TableScan
			self.target = obj
			self.cols = obj.keys()
		else:
			raise TypeError("Unsupport type")

		self.filterList = []
		self.counter = 0
		self.limitCount = -1
		
		self.lock = False
		self.cachedResult = None

	def __iter__(self):
		return self

	def __transform__(self, raw):
		flag = True
		for ft in self.filterList:
			flag = ft(raw)
			if flag == False:
				break
		if flag != False:
			self.counter += 1
			return { k:raw[k] for k in raw if k in self.cols }

		return None


	def __next__(self):
		# enumerate -> single process
		self.lock = True
		if self.limitCount != -1 and self.counter >= self.limitCount:
			raise StopIteration() 

		while True:
			raw = next(self.target)
			checked = self.__transform__(raw)
			if checked != None:
				return checked

	def project(self, *args):
		assert self.lock == False
		if isinstance(args[0], list):
			args = args[0]
		self.cols = [ c for c in self.cols if c in args ]

		return self

	def select(self, fn):
		assert self.lock == False
		assert callable(fn)
		self.filterList.append(fn)
		return self

	def limit(self, n):
		assert self.lock == False
		assert varType(n) == "int" and n > 0
		self.limitCount = n
		return self

	def show(self, limit = 5, truncate = False, trans = False):
		if trans == False:
			show(self, limit, truncate)
		else:
			data = []
			for i in range(limit):
				data.append(next(self))
			show( transpose(data) )

	def collect(self):
		# batch processing -> multiprocessing
		# try:
		return self.target.async(self.__transform__)
		# except:
		# 	print("enumerating...")
		# 	return list(self)


class TableScan():
	def __init__(self, tn):
		# read meta and file
		assert tn in __meta__
		self.meta = __meta__[tn]
		
		self.lock = False
		self.cols = self.meta["schema"].copy()

		self.cfp = None
		self.cfz = -1
		self.cfile = 0

	def __iter__(self):
		return self

	def __next__(self):

		if self.cfp != None:
			if self.cfp.tell() == self.cfz:
				self.cfp.close()
				self.cfp = None
				self.cfz = -1
				self.cfile += 1

		if self.cfile >= len(self.meta["fns"]):
			raise StopIteration() 

		if self.cfp == None:
			print("open " + self.meta["dir_prefix"] + self.meta["fns"][self.cfile])
			self.cfp = open( self.meta["dir_prefix"] + self.meta["fns"][self.cfile] )
			self.cfz = fstat(self.cfp.fileno()).st_size

		return entryReader( self.cfp.readline() )

	def async(self, operation):
		def run(fn):
			output = []
			with open(fn) as fp:
				# print("open " + fn)
				for line in fp.readlines():
					line = operation( entryReader(line) )
					if line != None:
						output.append( line )
			return output

		ret = []
		for res in parmap(target=run, inputs=[ self.meta["dir_prefix"] + fn for fn in self.meta["fns"] ] ):
			ret += res

		return ret


	def keys(self):
		return self.cols.keys()

