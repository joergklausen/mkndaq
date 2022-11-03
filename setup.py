from setuptools import setup

setup(
    name='gaw-mkn-daq',
    version='0.5.2',
    packages=['mkndaq', 'mkndaq.inst', 'mkndaq.tests', 'mkndaq.utils', 'sockslib'],
    url='',
    license='MIT',
    author='jkl',
    author_email='joerg.klausen@meteoswiss.ch',
    description='Data acquisition for MKN Global GAW station'
)
