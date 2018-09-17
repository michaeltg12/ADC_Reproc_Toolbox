#!/apps/base/python3/bin/python3

import os

# Automatically set the __all__ variable with all
# the available plugins.

# Use the current location of this file
# Split that path on '/' so you get each part
# Eliminate the last element since it is the file name
# Rejoin the remaining elements in the array with '/'
plugin_dir = '/'.join(os.path.realpath(__file__).split('/')[:-1])

__all__ = []
for filename in os.listdir(plugin_dir):
    filename = plugin_dir + '/' + filename
    if os.path.isfile(filename):
        basename = os.path.basename(filename)
        base, extension = os.path.splitext(basename)
        if (extension == '.py' or extension == '.pyc') and not basename.startswith('_') and basename not in __all__:
            __all__.append(base)
