# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name = 'sparktreecorr',
    description = 'Treecorr on Spark',
    url = 'https://github.com/ptallada/sparktreecorr',
    license = 'AGPL3',
    
    author = 'Pau Tallada Crespí',
    author_email = 'pau.tallada@gmail.com',
    
    packages = ['sparktreecorr'],
    
    install_requires = [
        'healpy',
        'numpy',
        'pandas',
        'pyspark',
        'treecorr',
    ],
    
    setup_requires = [
        'setuptools_scm',
    ],
    
    extras_require = {
        'dev' : [
            'jupyterlab',
        ]
    },
    
    
    zip_safe =True,
    use_scm_version = True,
)