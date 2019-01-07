# This does not work exactly right yet
from setuptools import setup, find_packages

setup(
    name='ADC_Reproc_Toolbox',
    version='0.1',
    author='Michael Giansiracusa, Alka Singh, Josh Ray',
    description='CLI and Workflow manager for reprocessing',
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'Click',
        'PyYaml'
    ],
    data_files=[
        ('', ['config/config.py',
              'config/logging_config.yaml',
              'config/.db_connect'])
    ],
    entry_points={
        'console_scripts': [
            'rtb = ADC_Reproc_Toolbox.rtb:main'
        ]
    }
)