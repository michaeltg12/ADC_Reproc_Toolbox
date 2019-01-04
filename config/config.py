from datetime import date
from os import environ
from re import compile
from time import time

version = 0.1
dqr_regex = compile(r"D\d{6}(\.)*(\d)*")
datastream_regex = compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|"
                                      r"tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|"
                                      r"rld|sgp|smt|twp|yeu)\w+\.(\w){2}")
reproc_home = environ.get('REPROC_HOME')
int(date.fromtimestamp(time()).strftime("%Y%m%d"))