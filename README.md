## Spark-Python-Egg-File

#What is python Egg File?

A “Python egg” is a logical structure embodying the release of a specific version of a Python project, comprising its code, resources, and metadata. There are multiple formats that can be used to physically encode a Python egg, and others can be developed. However, a key principle of Python eggs is that they should be discoverable and importable. That is, it should be possible for a Python application to easily and efficiently find out what eggs are present on a system, and to ensure that the desired eggs’ contents are importable.

#This is article will demonstrate how to create python code packaging with egg file build and execute the code on spark distributed framework. Firstly, we have few python modules files which have some functions or methods inside. The spark python application has below flow.

  1. Read movies crew and cast data from S3 location.

  2. Create Spark schema and data frame.

  3. Load Spark data frame into Hive tables.

  4. Read from hive Tables and show table record counts. 

#Environment:

  setuptools = 18.5
  Python = 2.7
  Spark = 2.4.3

#how to build egg file

  python setup.py bdist_egg

#how to run egg file:

/usr/lib/spark/bin/spark-submit --master yarn --py-files s3://jyadav-euc/SparkEggfile-0.1-py2.7.egg s3://jyadav-euc/hackathon/driver.py
