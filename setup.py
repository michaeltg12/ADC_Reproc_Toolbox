# This does not work exactly right yet
from setuptools import setup

setup(
    name='ADC_Reproc_Toolbox',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    description='CLI and Workflow manager for reprocessing',
    author='Michael Giansiracusa, Alka Singh, Josh Ray',
    install_requires=['PyYaml','Click'],
    py_modules=['rtb'],
    entry_point='''
        [console_scripts]
        rtb=ADC_Reproc_Toolbox.rtb:main
    ''',
)