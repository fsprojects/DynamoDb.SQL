README

For an overview of DynamoDB local please refer to the documentation at http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html 

Enhancements in this release 

This release provides support for Global Secondary Indexes(GSI). 

It also supports Java6 (previous release of this tool supported Java7 only) and includes fixes to issues reported by our customers  

Other changes

1. Running DynamoDB local
The command has changed slightly. The new command is
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar 

By default DynamoDB local runs on port 8000. If you want to change the port, please use the following command
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar  --port <port #  for e.g. 8091>

2.Backward compatibility with previous release of DynamoDB local

All your code and unit tests, you used to with the previous version of DynamoDB local will run unchanged with this release. 
   
In this release, the local database file format has changed; therefore, this release of DynamoDB Local will not be able to read data files created by older releases
