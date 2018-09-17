import os

def config():
	return {
		"begin": 20140501,
		"command": "test",
		"compare": True,
		"datastream": None,
		"duplicates": False,
		"end": 20140507,
		"facility": "C1",
		"ingest": True,
		"instrument": "mfrsr",
		"interactive": False,
		"job": "test",
		"quiet": False,
		"site": "sgp",
		"source": None,
		"stage": None,
		"username": os.environ.get('USER'),
		"vap": False
	}
