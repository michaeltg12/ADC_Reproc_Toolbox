#!/apps/base/python3/bin/python3

import re

# This is an example plugin that can be used as a
# skeleton for new plugins.
# The documentation string in the plugin class will be used to
# print(the help of the plugin.

from apm.pmanager.abstract import AbsPlugin

class Aoso3StagePlugin(AbsPlugin):
	''' Stage *AOSOZONE*.00 data when *AOSO3*.00 is selected for specific dates '''
	def __init__(self):
		self.config = None
		self.pattern = re.compile('^[a-z]{3}/([a-z]{3})aoso3([A-Z]\d+)\.00$')

	def hook_data_paths_alter(self, args):
		if not args['config']:
			return None
		self.config = args['config']

		if not args['data_paths']:
			return None
		self.data_paths = args['data_paths']

		new_paths = []

		for path in self.data_paths:
			m = self.pattern.match(path['output'])
			if m is not None:
				new_path = {
					'output': '%s/%saosozone%s.00' % (m.group(1), m.group(1), m.group(2)),
					'proc': path['proc'],
					'input': path['input'],
				}
				new_paths.append(new_path)

		for path in new_paths:
			self.data_paths.append(path)
		return self.data_paths

def load():
	""" Loads the current plugin """
	return Aoso3StagePlugin()
