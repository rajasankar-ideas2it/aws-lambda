# Introduction

For the purpose of polling the message from SQS queue , and fires up a new lambda worker asynchronously for each message.

This Project aims to develop:

- It requests messages from SQS queue, and fires up a new lambda worker asynchronously for each message. 
- Each worker is responsible for doing decrypt the messages and do the stuff based on payload and cleaning up SQS queue.

# Building the application

* To clean the target directory

	mvn clean

* To clean and build the Jar file

        mvn clean install

# Need to set Lambda function in Environment variables

Environment variables  | Description
-----------------------|-----------------------------------------------
queueUrl               | Used to get Message from get given queueUrl
queueName              | Used to get Message from given queueName
lambdaWorkerARN        | Worker arn url (Lambda function)
AWSAccessKey           | AWS credentials access key
AWSSecretKey           | AWS credentials secret key
SQSRegion              | SQS Region. For eg. ap-southeast-1
workerRegion           | Worker Region. For eg. ap-southeast-1

