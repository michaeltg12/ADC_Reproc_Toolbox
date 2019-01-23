# FIXME Trying to use PyInquirer, may not be best


import argparse
import re
from config.config import *
from datetime import datetime
from PyInquirer import style_from_dict, Token, prompt
from PyInquirer import Validator, ValidationError


class DQRNumberValidator(Validator):
    def validate(self, document):
        ok = dqr_regex.match(document.text)
        if not ok:
            raise ValidationError(
                message='Please enter a valid DQR number',
                cursor_position=len(document.text)
            )  # Move cursor to end

class DatastreamValidator(Validator):
    def validate(self, document):
        ok = datastream_regex.match(document.text)
        if not ok:
            raise ValidationError(
                message='Please enter a valid datastream',
                cursor_position=len(document.text) # Move cursor to end
            )

class DateValidator(Validator):
    def validate(self, document):
        for fmt in ('%Y-%m-%d', '%Y%m%d', '%d.%m.%Y', '%d/%m/%Y'):
            try:
                return datetime.strptime(document.text, fmt)
            except ValueError:
                pass
        raise ValidationError(
            message='Please enter a valid datastream',
            cursor_position=len(document.text)
        ) # Move cursor to end

def interact(questions=None, style=None):
    default_style = style_from_dict(
        {Token.QuestionMark: '#E91E63 bold', Token.Selected: '#673AB7 bold', Token.Instruction: '',
         Token.Answer: '#2196f3 bold', Token.Question: '', })
    default_questions = [
        {'type': 'confirm', 'name': 'woops', 'message': 'We forgot to pass in a question. Would you like to '
                                                        'email the developer?', 'default': False}, ]
    if questions == None: questions = default_questions
    if style == None: style = default_style
    answers = prompt(questions, style=style)
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