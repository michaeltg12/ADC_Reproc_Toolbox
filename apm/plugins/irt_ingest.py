#!/apps/base/python3/bin/python3

# This is an example plugin that can be used as a
# skeleton for new plugins.
# The documentation string in the plugin class will be used to
# print(the help of the plugin.

from apm.pmanager.abstract import AbsPlugin

class IrtIngestPlugin(AbsPlugin):
	''' Determine if the irt ingest or the irthr ingest should be used '''
	def __init__(self):
		self.processes = None
		self.irt = [
			'sgpirtC1',
			'sgpirt10mC1',
			'sgpirt25mC1',
		]

	def hook_ingest_alter(self, args):
		''' Alter the ingest used if necessary '''
		# Parse the args
		if 'processes' in args.keys():
			self.processes = args['processes']
		else:
			return None

		for k, v in enumerate(self.processes):
			ingest = v['ingest']

			# Only continue if irt_ingest was selected
			if ingest.split('/')[-1] != 'irt_ingest':
				break


			if v['input'].split('/')[-1].split('.')[0] not in self.irt:
				# Change the ingest to use irthr_ingest
				ingest = ingest.split('/')
				ingest[-1] = 'irthr_ingest'
				self.processes[k]['ingest'] = '/'.join(ingest)

				self.processes[k]['multiple'] = False


		return self.processes

def load():
	""" Loads the current plugin """
	return IrtIngestPlugin()

