from setuptools import setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='redcapdata',
    url='https://github.com/pmwaniki/redcap-data',
    author='Paul Mwaniki',
    author_email='pmmwaniki06@gmail.com',
    # Needed to actually package something
    py_modules=['redcapdata'],
    # Needed for dependencies
    install_requires=['numpy','pandas','grequests','requests'],
    # *strongly* suggested for sharing
    version='0.1',
    # The license can be anything you like
    license='MIT',
    description='Import data from redcap',
    # We will also need a readme eventually (there will be a warning)
    # long_description=open('README.txt').read(),
)