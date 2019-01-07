# May not be necessare.
# I prefer to explicitly import from main module unless the path is long.

import sys
sys.path.append("..")

from . import rename
from . import stage

from .rename import *
from .stage import *