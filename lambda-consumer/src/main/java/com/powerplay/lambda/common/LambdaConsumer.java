/*
 * Copyright 2016, Powerplay, Inc. Date: 10 Dec, 2016.
 */
package com.powerplay.lambda.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.powerplay.lambda.util.LambdaUtil;

/**
 * This class acts as a consumer for all incoming streams from the attached
 * event scheduler with <code>rate(1 minutes)</code>. It requests messages from
 * <b>SQS</b> queue, and fires up a new lambda worker asynchronously for each
 * message. Each worker is responsible for doing decrypt the messages and
 * call rest service and cleaning up <b>SQS</b> queue.
 */
public class LambdaConsumer implements RequestHandler<Map<String, Object>, String> {

    // Property files are getting from environment variables
    private static final String SQS_QUEUE_URL = System.getenv("queueUrl");
    private static final String SQS_QUEUE_NAME = System.getenv("queueName");
    private static final String LAMBDA_WORKER_ARN = System.getenv("lambdaWorkerARN");
    private static final String AWS_ACCESS_KEY = System.getenv("AWSAccessKey");
    private static final String AWS_SECRET_KEY = System.getenv("AWSSecretKey");
    private static final String SQS_REGION = System.getenv("SQSRegion");
    private static final String WORKER_REGION = System.getenv("workerRegion");

    // LambdaLogger API provided by AWS is used for the logging throughout
    private static LambdaLogger logger;
    private static AmazonSQSClient sqsClient;
    private static AWSCredentials credentials;

    /**
     * Acts as an entry point for the lambda consumer, payload parameter is
     * configured as a constant JSON text in AWS console.
     * 
     * @param payload
     *            Constant configured payload - unused!
     * @param context
     *            Context of the lambda function
     * 
     * @return Showing success message if Lambda consumer executed
     * 
     */
    public String handleRequest(Map<String, Object> payload, Context context) {
    	
    	logger = context.getLogger();
    	
    	logger.log("Lambda consumer invoked for the queue name : " + SQS_QUEUE_NAME);
        // Initialize AWS credentials
        credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);

        // Initialize the SQS client
        sqsClient = new AmazonSQSClient(credentials);

        // Used to set SQS Region from Environment variables
        sqsClient.setRegion(Region.getRegion(Regions.fromName(SQS_REGION)));

        requestSqsMessage(context);
        logger.log("Lambda consumer successfully executed!");
        return "Lambda consumer successfully executed!";
    }

    /**
     * <p>
     * Get SQS Message from queue and process SQS message Lambda worker.
     * </p>
     * 
     * @param context
     *            - Used to call repeatedly SQS Queue every 10 seconds
     */
    private void requestSqsMessage(Context context) {
        // Receive messages from SQS with the the max Limit
        final ReceiveMessageRequest request = new ReceiveMessageRequest(SQS_QUEUE_URL);
        request.setMaxNumberOfMessages(10);

        // lambda execution time
        request.setWaitTimeSeconds(20);

        logger.log("Try to receive messages from SQS queue, endpoint: " + SQS_QUEUE_URL + " \n");
        final List<Message> messages = sqsClient.receiveMessage(request).getMessages();
        for (Message message : messages) {
            if (null != message.getBody() && !message.getBody().isEmpty()) {
                logger.log("Currently processing SQS message, MessageId: " + message.getMessageId() + " \n");
                
                final ObjectMapper mapper = new ObjectMapper();
                final ObjectNode queueDetails = mapper.createObjectNode();
                queueDetails.put("SQSQueueUrl", SQS_QUEUE_URL);
                queueDetails.put("SQSRegion", SQS_REGION);
                queueDetails.put("SQSQueueName", SQS_QUEUE_NAME);
                Map<String, Object> msg= new HashMap<String, Object>();
                
                final InvokeRequest invokeRequest = new InvokeRequest();
                invokeRequest.setFunctionName(LAMBDA_WORKER_ARN);
                invokeRequest.setInvocationType(InvocationType.Event);
                
                // Serialize SQS message to String in order to fire the lamdba
                // worker since by default the payload should be String
                try {
                	//String msgStr = LambdaUtil.toString(message);
                	msg = mapper.readValue(message.getBody(), new TypeReference<Map<String, Object>>(){});
                	queueDetails.put("rawData", message.getBody());
                	message.setBody(queueDetails.toString());
                    invokeRequest.setPayload("\"" + LambdaUtil.toString(message) + "\"");
                } catch (IOException e) {
                    logger.log("Error in serializing the SQS message to String, MessageId : " + message.getMessageId()
                            + "\n Exception Message : " + e.getMessage());
                }

                // Initialize lamdba client
                final AWSLambdaClient lambdaClient = new AWSLambdaClient(credentials);
                lambdaClient.setRegion(Region.getRegion(Regions.fromName(WORKER_REGION)));
                logger.log("Invoking lambda worker on SQS message, MessageId: " + message.getMessageId() + " \n");
                final InvokeResult invokeResult = lambdaClient.invoke(invokeRequest);
                logger.log("Lambda worker succesfully executed on SQS message, MessageId: " + message.getMessageId()
                        + "; StatusCode : " + invokeResult.getStatusCode() + "\n");
            }
        }
        // If consumer function execution time remaining > 10s, get more SQS
        // messages
        if (context.getRemainingTimeInMillis() > 10000) {
            requestSqsMessage(context);
        }
    }
}
