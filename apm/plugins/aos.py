#!/apps/base/python3/bin/python3

# This is an example plugin that can be used as a
# skeleton for new plugins.
# The documentation string in the plugin class will be used to
# print(the help of the plugin.

from apm.pmanager.abstract import AbsPlugin


class AOSPlugin(AbsPlugin):
	""" An example plugin that prints dummy messages """
	def __init__(self):
		pass

	# Public methods will be considered plugin commands.
	# The name of the command will be the method name.
	# The documentation string in command methods will be used to
	# print(the help of the command.
	# The arguments are the options given to the command itself
	def dummy(self, args):
		""" Prints a dummy message """
		print("This is the print_handler in the example plugin")


# Each plugin must provide a load method at module level that will be
# used to instantiate the plugin
def load():
	""" Loads the current plugin """
	return AOSPlugin()
