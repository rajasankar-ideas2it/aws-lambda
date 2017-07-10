# Introduction

For the purpose worker is responsible for doing decrypt the messages and do the stuff based on payload and cleaning up SQS queue.

This Project aims to develop:

- Worker is responsible for doing decrypt the messages and do the stuff based on payload and cleaning up SQS queue.

# Building the application

* To clean the target directory

	mvn clean

* To clean and build the Jar file

        mvn clean install

# Need to set Lambda function in Environment variables

Environment variables  | Description
-----------------------|-----------------------------------------------
AWSAccessKey           | AWS credentials access key
AWSSecretKey           | AWS credentials secret key
keyRegion              | Key Region. For eg. ap-southeast-1

