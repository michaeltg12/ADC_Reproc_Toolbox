# May not be necessare.
# I prefer to explicitly import from main module unless the path is long.

from . import rename
from . import stage

from .rename import rename
from .rename import printStuff
from .stage import stage