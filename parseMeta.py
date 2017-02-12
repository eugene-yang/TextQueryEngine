# create the metadata for each big tables

from os import listdir
import json
import sys
from util import *

# table list
tableList = [ d for d in listdir("../") if d.split("_")[0]=="all" ]
if len(sys.argv) > 1:
	tableList = sys.argv[1:]


for tn in tableList:
	print("parsing " + tn + "...")

	fileList = [ f for f in listdir("../"+tn) if f.split("-")[0] == "part" ]
	# parse structure
	testLine = entryReader( open("../"+tn+"/"+fileList[0]).readline() )

	schema = { cn: varType(testLine[cn]) for cn in testLine.keys() }

	json.dump({
		"dir": tn,
		"fns": fileList,
		"schema": schema
	}, open("./metadata/" + tn + ".meta", "w"))
