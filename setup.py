from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='queuing-hub',  # Required
    version='0.0.0',  # Required
    description='Multi-cloud Queuing Hub',  # Optional
    long_description=long_description,  # Optional
    long_description_content_type='text/markdown',  # Optional
    url='https://github.com/tosh223/py-queuing-hub',  # Optional
    author='Toshifumi Tsutsumi',  # Optional
    classifiers=[  # Optional
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='aws, gcp, queue, development',  # Optional
    packages=find_packages(),  # Required
    python_requires='>=3.6, <3.10',
    install_requires=['google-cloud-pubsub', 'google-cloud-monitoring', 'boto3'],  # Optional
    # extras_require={  # Optional
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },
    # package_data={  # Optional
    #     'sample': ['package_data.dat'],
    # },
    # data_files=[('my_data', ['data/data_file'])],  # Optional
    # project_urls={  # Optional
    #     'Bug Reports': 'https://github.com/pypa/sampleproject/issues',
    #     'Funding': 'https://donate.pypi.org',
    #     'Say Thanks!': 'http://saythanks.io/to/example',
    #     'Source': 'https://github.com/pypa/sampleproject/',
    # },
)