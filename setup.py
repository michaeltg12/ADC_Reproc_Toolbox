from setuptools import setup

setup(
    name='rtb',
    version='0.1',
    description='CLI and Workflow manager for reprocessing',
    author='Michael Giansiracusa, Alka Singh, Josh Ray',
    packages=['commands','documentation','support','tools'],
    data_files=[('config', ['.config/.arm_db_connect','.config/.db_connect','.config/logging_config.yaml'])],
    install_requires=['PyYaml','click'],
    py_modules=['rtb'],
    entry_points={
        'console_scripts': ['rtb = rtb:main',],
    }
)