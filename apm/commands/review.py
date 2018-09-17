#!/apps/base/python3/bin/python3

import os

from apm.pmanager.manager import PluginManager

from apm.classes.system import dir_pattern
from apm.classes.system import jprint
from apm.classes.db import DB

class Review:
	""" Send the needed files to a reviewer """

	def __init__(self, config, files=None):
		""" Initialize with args """
		self.config = config
		self.files = files
		self.db = DB(self.config)


	def run(self):
		""" Run the review portion of the cleanup phase """
		self.config['cleanup_status']['review']['status'] = True
		return self.config, self.files
