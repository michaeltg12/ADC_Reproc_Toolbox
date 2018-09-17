#!/apps/base/python3/bin/python3

import sys
import readline

import unittest
import mock


class UI:
	""" User Interface """

	def __init__(self):#, config):
		""" Initialize with args """
		# self.config = config

	def yn_choice(self, message, default='y'):
		"""
			Ask a question with a Y/n or y/N answer. Capital letter is default.
			Return True if answer is YES
			Return False if answer is NO
		"""
		choices = 'Y/n' if default.lower() in ('y', 'yes') else 'y/N'
		choice = input("{0} ({1}) ".format(message, choices))
		values = ('y', 'ye', 'yes', '') if default == 'y' else ('y', 'ye', 'yes')
		return choice.strip().lower() in values

	def progress(self, progress):
		done = progress/2
		remain = 50 - done
		line = '\r[{}{}] {}%'.format('#'*int(done), ' '*int(remain), progress)
		print(line, end="")
		sys.stdout.flush()

	def question(self):
		pass

	def message(self):
		pass

################################################################################
# Unit tests
################################################################################
class TestYNChoice(unittest.TestCase):
	def test_1(self):
		""" Default Y -> pass y -> Return True """
		ui = UI()
		with mock.patch('__builtin__.input', side_effect=['y']):
			result = ui.yn_choice('', 'y')
			expected = True

			print('Result:   {}\nExpected: {}'.format(result, expected))
			assert result == expected

	def test_2(self):
		""" Defualt Y -> pass n -> Return False """
		ui = UI()
		with mock.patch('__builtin__.input', side_effect=['n']):
			result = ui.yn_choice('', 'y')
			expected = False

			print('Result:   {}\nExpected: {}'.format(result, expected))
			assert result == expected

	def test_3(self):
		""" Default Y -> pass '' -> Return True """
		ui = UI()
		with mock.patch('__builtin__.input', side_effect=['']):
			result = ui.yn_choice('', 'y')
			expected = True

			print('Result:   {}\nExpected: {}'.format(result, expected))
			assert result == expected

	def test_4(self):
		""" Default N -> pass y -> Return True """
		ui = UI()
		with mock.patch('__builtin__.input', side_effect=['y']):
			result = ui.yn_choice('', 'n')
			expected = True

			print('Result:   {}\nExpected: {}'.format(result, expected))
			assert result == expected

	def test_5(self):
		""" Defualt N -> pass n -> Return False """
		ui = UI()
		with mock.patch('__builtin__.input', side_effect=['n']):
			result = ui.yn_choice('', 'n')
			expected = False

			print('Result:   {}\nExpected: {}'.format(result, expected))
			assert result == expected

	def test_6(self):
		""" Default N -> pass '' -> Return False """
		ui = UI()
		with mock.patch('__builtin__.input', side_effect=['']):
			result = ui.yn_choice('', 'n')
			expected = False

			print('Result:   {}\nExpected: {}'.format(result, expected))
			assert result == expected

################################################################################
if __name__ == '__main__':
	unittest.main(buffer=True)
	# unittest.main()

