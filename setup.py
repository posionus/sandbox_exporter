# coding=utf-8
import setuptools

setuptools.setup(
    name='sandbox_exporter',
    version='0.0.13',
    scripts=['sandbox_exporter/exporter.py'],
    author="Chueh Lien",
    author_email="lien_julia@bah.com",
    description="S3 select utility package",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/usdot-its-jpo-data-portal/sandbox_exporter",
    packages=['sandbox_exporter'],
    classifiers=[
        'Programming Language :: Python :: 3.7',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['requests[security]>=2.18.3',
                      'pyasn1>=0.4.2',
                      'boto3>=1.7.79']
)
