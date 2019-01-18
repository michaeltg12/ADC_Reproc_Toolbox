# FIXME Trying to use PyInquirer, may not be best


import argparse
import re
from datetime import datetime
from PyInquirer import style_from_dict, Token, prompt
from PyInquirer import Validator, ValidationError

DQR_REGEX = compile(r"D\d{6}(\.)*(\d)*")
DATASTREAM_REGEX = compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|"
                              r"tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|"
                              r"rld|sgp|smt|twp|yeu)\w+\.(\w){2}")


class DQRNumberValidator(Validator):
    def validate(self, document):
        ok = DQR_REGEX.match(document.text)
        if not ok:
            raise ValidationError(
                message='Please enter a valid DQR number',
                cursor_position=len(document.text)
            )  # Move cursor to end

class DatastreamValidator(Validator):
    def validate(self, document):
        ok = DATASTREAM_REGEX.match(document.text)
        if not ok:
            raise ValidationError(
                message='Please enter a valid datastream',
                cursor_position=len(document.text) # Move cursor to end
            )

class DateValidator(Validator):
    def validate(self, document):
        for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
            try:
                return datetime.strptime(document.text, fmt)
            except ValueError:
                pass
        raise ValidationError(
            message='Please enter a valid datastream',
            cursor_position=len(document.text)
        ) # Move cursor to end

class Interactive(object):
    def __init__(self):
        self.style = style_from_dict(
            {Token.QuestionMark: '#E91E63 bold', Token.Selected: '#673AB7 bold', Token.Instruction: '',  # default
             Token.Answer: '#2196f3 bold', Token.Question: '', })

        self.questions = [
            {'type': 'confirm', 'name': 'woops', 'message': 'We forgot to pass in a question. Would you like to '
                                                            'email the developer?', 'default': False},
        ]
        # Examples #
        # self.questions = [
        #     {'type': 'confirm', 'name': 'reprocessing', 'message': 'Is this a DQR reprocesssing job?',
        #      'default': False},
        #     {'type': 'input', 'name': 'job', 'message': 'What is the DQR #?',
        #         'validate': DQRNumberValidator},
        #     {'type': 'list', 'name': 'command', 'message': 'Which command/task do you want to run?',
        #         'choices': ['vapinfo', 'stage', 'email'], 'filter': lambda val: val.lower()},
        #     {'type': 'editor', 'name': 'datastreams', 'message': 'What are the datastreams?',
        #         'eargs': {'editor': '/usr/bin/vim', 'save': True, 'filename': 'test_editor.txt'}}
        #     ]

    def interact(self, questions=None, style=None):
        if questions == None: questions = self.questions
        if style == None: style = self.style
        answers = prompt(questions, style=self.style)
        return answers


###################### Example of making custom actinos ###################
# class PromptAction(argparse.Action):
#     def __call__(self, parser, namespace, value, option_string=None):
#         self.validate(parser, value)
#         setattr(namespace, self.dest, value)
#
#     @staticmethod
#     def validate(parser, value):
#         if value not in ('foo', 'bar'):
#             parser.error('{} not valid column'.format(value))
#
#
# parser = argparse.ArgumentParser()
# parser.add_argument('--columns', action=PromptAction)
# args = parser.parse_args()
# if args.columns is None:
#     args.columns = input('Enter columns: ')
#     PromptAction.validate(parser, args.columns)
# print(args.columns)