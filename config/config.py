import re
from datetime import date
from os import environ
from time import time

version = 0.1
dqr_regex = re.compile(r"D\d{6}(\.)*(\d)*")
datastream_regex = re.compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|rld|sgp|smt|twp|yeu)\w+[A-Z]{1}[0-9]+\.(\w){2}")
reproc_home = environ.get('REPROC_HOME')
post_proc = '{}/post_processing'.format(reproc_home)
test_reproc_home = '/data/home/giansiracusa/test_rtb_dir'
test_post_proc = '/data/home/giansiracusa/test_rtb_dir/post_processing'
today = int(date.fromtimestamp(time()).strftime("%Y%m%d"))

init_paths = [
    '{reproc_home}',
    '{reproc_home}/{job}',
    '{reproc_home}/{job}/archive',
    '{reproc_home}/{job}/collection',
    '{reproc_home}/{job}/conf',
    '{reproc_home}/{job}/datastream',
    '{reproc_home}/{job}/db',
    '{reproc_home}/{job}/file_comparison',
    '{reproc_home}/{job}/file_comparison/raw',
    '{reproc_home}/{job}/file_comparison/tar',
    '{reproc_home}/{job}/health',
    '{reproc_home}/{job}/logs',
    '{reproc_home}/{job}/out',
    '{reproc_home}/{job}/quicklooks',
    '{reproc_home}/{job}/scripts',
    '{reproc_home}/{job}/tmp',
    '{reproc_home}/{job}/www',
    '{post_proc}',
    '{post_proc}/{job}',
]

default_job_conf = {
    "alias": None,
    "command": None,
    "compare": True,
    "datastreams": {
    },
    "db_up": True,
    "demo": None,
    "devel": False,
    "exit": False,
    "interactive": False,
    "job": None,
    "rename": True,
    "stage": None,
    "username": None
}

default_ds_conf = {
    "start": None,
    "end": None,
    "site": None,
    "instrument": None,
    "facility": None,
    "duplicates": False,
    "stage": {
    "source": None
    },
    "ingest": {
    "command": None,
    "status": None
    },
    "vap": {
    "command": None,
    "status": None
    },
    "archive": {
    "files_deleted": False,
    "files_released": False,
    "move_files": False,
    "status": False
    },
    "cleanup": {
    "files_archived": False,
    "files_cleaned_up": False,
    "status": False
    },
    "remove": {
    "archive_list": False,
    "deletion_list": False,
    "files_bundled": False,
    "status": False
    },
    "review": {
    "status": True
    }
}

data_files = {'env.bash':
'''setenv DATA_HOME "/data/project/0021718_1509993009/D181011.5"
setenv COLLECTION_DATA "$DATA_HOME/collection"
setenv CONF_DATA "$DATA_HOME/conf"
setenv LOGS_DATA "$DATA_HOME/logs"
setenv HEALTH_DATA "$DATA_HOME/health"
setenv DB_DATA "$DATA_HOME/db"
setenv ARCHIVE_DATA "/data/archive"
setenv DATASTREAM_DATA "$DATA_HOME/datastream"
setenv WWW_DATA "$DATA_HOME/www"
setenv OUT_DATA "$DATA_HOME/out"
setenv QUICKLOOK_DATA "$DATA_HOME/quicklooks"
setenv TMP_DATA "$DATA_HOME/tmp" ''',
              'env.csh' :
'''DATA_HOME="{reproc_home}/{job}"
COLLECTION_DATA="$DATA_HOME/collection"
CONF_DATA="$DATA_HOME/conf"
LOGS_DATA="$DATA_HOME/logs"
HEALTH_DATA="$DATA_HOME/health"
DB_DATA="$DATA_HOME/db"
ARCHIVE_DATA="/data/archive"
DATASTREAM_DATA="$DATA_HOME/datastream"
WWW_DATA="$DATA_HOME/www"
OUT_DATA="$DATA_HOME/out"
QUICKLOOK_DATA="$DATA_HOME/quicklooks"
TMP_DATA="$DATA_HOME/tmp" '''
             }
























































































































































































import random
if random.randint(0,1000) == 42:
    c=""" Space Kitten!!!
;:::cccllooodddxxkko..,.      ,kXXXXXNNNNNNNNNNNNNNNNNNNNNNNNXXNKo.        ;kOkkxxxdddoolllccc:::;;;
:::cccllooodddxxkkOx,':,..     .cOXNNNNNNNNNNNNNNNNNNNNNNNNNNN0d;.     ..  :OOOkkkxxdddooollccc:::;;
::ccllloooddxxxkkO0k:;ol,....    .dKK00KXKKXXXXXXKK0KKK0KXXKk:.      ..';..o00OOOkkxxxddooolllcc:::;
:ccllloodddxxkkkO000dcdkdl;'...  .,coccokxxO0OkO0Oxddkdlloxo'         .:l',OK00OOOkkkxxdddoolllcc:::
ccllloodddxxkkOOO0KKOox0Oxo:,'.....';:cloooooxxxdooooxxl:;,,,..  .....,ll;oKKK000OOkkkxxdddoolllcc::
clllooddxxxkkOO00KKKXkxkkxdl:;'...,,;;lxxl:cclooloollxko:,'.....''..';ldldXXXKK000OOOkkxxxddoolllcc:
lllooddxxxkkOOO0KKKXXKd;,cl:,'.',;,,;;lxdol:cc:ccc:lxkkoc:,,'..,:ll;;:ocoKNXXXKKK00OOOkkxxdddoollccc
lloodddxxkkOOO00KKXXNKd:,::,',,,,;;;,,,:llc:;;,;;;:clol:;;;:;,,,,;cc,',cONNNXXXKKK00OOOkkxxdddoollcc
loooddxxkkkOO00KKXXNNXk:'......',:ll:;,;c;,;;;;,,,;;;::;;;,'.........';oOXNNNXXXKKK00OOkkkxxddoolllc
looddxxkkkOO00KKXXXNNk,.        ..';:,',;,'',;;;,'''',,,'.            .,dKNNXNNXXKK000OOkkxxxddoollc
oodddxxkkOO000KXXXN0c.        ...''.',;:;'..',,;,'..':,.                .oKNXNNXXXKK000OOkkxxddoooll
ooddxxkkkOO00KKXXNO,     .  .....;;''.'coc;,,;c:,...;:.                  'dKNNNXXXKKK00OOkkkxxddooll
ooddxxkkOO000KKXNKc         ...  ..,,',;lxollodc;,,,:'                   .;OXNXXXXXKK00OOOkkxxddooll
odddxxkkOO00KKK0Oo,          ........,cc;;cdOOOxdl:,..                    .cdxOXNXXKKK00OOkkxxddoool
oddxxxkkOO00KKko;...            .....'::' .lOOOkxc'...                    .,:oOXNXXXKK00OOkkxxxddool
oddxxkkkOO00KKXX0dc'...          ..,'.';,';x0OOOkl;'''                   .;xKXXXXXXXKK00OOkkkxxddool
oddxxkkOOO00KKXXNNOc.            ..,;,,..,clc::::::;'...                 'lOXXNNXXXXKK00OOkkkxxddool
oddxxkkOOO00KKXXXXKx:.            .:c;..:c'.     .'cl'...             ..,ck0XXNNXXXXKK00OOkkxxxddool
oddxxxkkOO00KKKXXXK0xl,..       ...'.':oxxdc'. .:odxxdc,.....     ...',',okOKXXNXXXKKK00OOkkxxxddool
odddxxkkOO000KKXX0OOxc;'............;lxkkOOko',okkxxxxddl,..............:odx0XXNXXXKKK00OOkkxxddoool
ooddxxkkOO000KKKKOxkkc'...     ..';oxxkkkkdl:.'clxkkxxddol;...     ...',,;cd0KXNXXXKK00OOOkkxxddooll
ooddxxkkkOO00KKK0kooxo;'.......,:cclloddooc,....:looooool::c:,......','...;okKXXXXKKK00OOkkxxxddooll
oooddxxkkOO00000kxlclol:::;,,'...',,:c:;,....''...',::c:,,,,'..',,,,..  ..,:oOKXXKKK00OOOkkxxddoolll
looddxxxkkOO000Oxoc;',clc:,,;;'.  ........''..','........... ..''''.......,:okKKXKK000OOkkxxxddoollc
llooddxxkkkOO0OOkoc;,'..',;;,'...     ..''''..',.....       ...   .....'',;;:lkKKK000OOkkkxxddoolllc
lloooddxxkkkOOko:;;,''..  ...'''...       ......                   ...'',;;;cok0K000OOkkkxxddooollcc
clloodddxxkkkOkdol:;,'...     .....                              ....',,,;;;:lx0000OOkkkxxdddoollccc
cclloodddxxkkkdl:,'''...                                      .......,,,',;;;:oO00OOkkkxxdddoollccc:
ccllloodddxxxdl;,'............                                ......''..',,'';lxOOOkkxxxdddoolllcc::
:ccclloooddxxoc,''''''..........                            ...........,;,'',,;okOkkxxxddooollccc:::
::ccclloooddoc,...................                 ...................',,'''..'cdkkxxdddooollccc:::;
:::cccllooooo:,...........   ......          .. .  ...  ........ .......'.....',lxxxdddooollccc:::;;
"""
else:
    c=''