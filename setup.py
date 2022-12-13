from setuptools import setup, find_packages

test_packages = [
    'pytest==7.2.0',
    'pytest-cov==4.0.0'
    'pytest-mysql==2.3.1'
]

docs_packages = [
    'mkdocs==1.1',
]

base_packages = [
    'mysql-connector-python==8.0.31',
    'slack-sdk==3.19.2',
    'python-crontab==2.6.0',
    'python-amazon-sp-api==0.17.5',
    'apache-airflow==2.5.0',
    'apache-airflow-providers-cncf-kubernetes==5.0.0',
    'dacite==1.6.0',
    'pytest==7.1.1',
]

dev_packages = docs_packages + test_packages

setup(
    name='amazon-image-checker',
    packages=find_packages(),
    version='0.0.1',
    url='https://github.com/ohadnav/amazon-image-checker',
    license='',
    author='ohad',
    author_email='ohadnav@gmail.com',
    description='Monitoring changes of images of Amazon products',
    install_requires=base_packages,
    extras_require={
        'test': test_packages,
        'dev': dev_packages,
    },
    python_requires='==3.9',
)
