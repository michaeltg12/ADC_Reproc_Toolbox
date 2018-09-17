#!/apps/base/python3/bin/python3

# This is an example plugin that can be used as a
# skeleton for new plugins.
# The documentation string in the plugin class will be used to
# print(the help of the plugin.

from apm.pmanager.abstract import AbsPlugin


class CombinePlugin(AbsPlugin):
	"""Example plugin for SASZE and SASHE"""
	def __init__(self):
		self.site = None
		self.instrument = None
		self.facility = None

	# Public methods will be considered plugin commands.
	# The name of the command will be the method name.
	# The documentation string in command methods will be used to
	# print(the help of the command.
	# The arguments are the options given to the command itself
	def hook_get_datastreams(self, options):
		"""Get the vis and nir versions of the datastream"""
		if not options['args']:
			exit("Expected args but not passed. Please try again")

		args = options['args']
		if args['instrument'] == 'sasze' or args['instrument'] == 'sashe':
			print("oops")
			self.site = args['site']
			self.instrument = args['instrument']
			self.facility = args['facility']

			args['datastream'] = []
			args['datastream'].append('{}{}*{}.00'.format(args['site'], args['instrument'], args['facility']))

			args['site'] = None
			args['instrument'] = None
			args['facility'] = None

			return args
		else:
			return


	def hook_pre_unpack(self, options):
		"""change the input for the vis and nir outputs"""
		if not options['args']:
			exit("OOPS")

		if self.instrument != None and (self.instrument == 'sasze' or self.instrument == 'sashe'):
			print(self.site)
			print(self.instrument)
			print(self.facility)

		return

# Each plugin must provide a load method at module level that will be
# used to instantiate the plugin
def load():
	""" Loads the current plugin """
	return CombinePlugin()
