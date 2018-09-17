#!/apps/base/python3/bin/python3

# This is an example plugin that can be used as a
# skeleton for new plugins.
# The documentation string in the plugin class will be used to
# print(the help of the plugin.

from apm.pmanager.abstract import AbsPlugin

class MfraafStagePlugin(AbsPlugin):
	''' Stage SGPIAPMFRC1.00 data when SGPMFRAAFF2.00 is selected for specific dates '''
	def __init__(self):
		self.config = None

	def hook_data_paths_alter(self, args):
		''' Make sure sgpiapmfrC1 is included in the datapaths '''
		if not args['config']:
			return None
		self.config = args['config']

		if not args['data_paths']:
			return None
		self.data_paths = args['data_paths']

		new_paths = []

		for path in self.data_paths:
			if path['output'] == 'sgp/sgpmfraafF2.00':
				new_path = {
					'output': 'sgp/sgpiapmfrC1.00',
					'proc': path['proc'],
					'input': path['input'],
				}
				new_paths.append(new_path)

		for path in new_paths:
			self.data_paths.append(path)
		return self.data_paths

def load():
	""" Loads the current plugin """
	return MfraafStagePlugin()
