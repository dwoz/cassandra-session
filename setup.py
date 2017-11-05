from setuptools import setup

test_requires = [
    'tox',
    'pytest',
]

setup(
    name = 'cassandra-session',
    py_modules = ['cassandra_session'],
    version = '0.0.1',
    description = (
        'Cassandra sessions'
    ),
    author = 'dan@woz.io',
    author_email = 'dan@woz.io',
    url = 'https://github.com/dwoz/cassandra-session',
    classifiers=[
        'Programming Language :: Python',
    ],
    install_requires=[
        'cassandra-driver',
        'beaker',
    ]
)

