/*
 * Copyright 2016, powerplay, Inc. Date: 10 Dec, 2016.
 */
package com.powerplay.lambda.common.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.powerplay.lambda.util.JsonUtil;
import com.powerplay.lambda.util.LambdaUtil;

/**
 * This class is basically a worker lambda thread that gets fired from
 * {@link LambdaConsumer} asynchronously for each <code>SQS</code> message. It
 * parses the incoming payload, deciphers it and invokes report rest api call
 * 
 * @author Rajasankar
 *
 */
public class ServiceWorker implements RequestHandler<String, Object> {

    private static String SQS_QUEUE_URL;
    private static String SQS_QUEUE_NAME;
    private static String SQS_REGION;
    private static final String AWS_ACCESS_KEY = System.getenv("AWSAccessKey");
    private static final String AWS_SECRET_KEY = System.getenv("AWSSecretKey");
    private static final String KEY_REGION = System.getenv("keyRegion");
    private static final String DECRYPTION_NEED = System.getenv("decryptionNeed");
    public static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_TYPE_APPLICATION_FORM_URLENCODED = "application/x-www-form-urlencoded";
    private static LambdaLogger logger;
    private static AmazonSQSClient client;

    /**
     * Acts as an entry point for worker thread, input parameter is coming from
     * the consumer thread {@link LambdaConsumer}, which is serialized
     * <code>SQS</code> message into a string.
     * 
     * @param input
     *            Serialized SQS message
     * @param context
     *            Context of the worker thread
     * @return successMessage - - Showing success message if Lambda worker
     *         executed
     */
    public String handleRequest(String input, Context context) {
        
        // LambdaLogger API provided by AWS is used for the logging throughout
        logger = context.getLogger();

        // Initialize the SQS client
        client = new AmazonSQSClient();

        // Deserialize JSON input into SQS Message object
        try {
            processWorker((Message) LambdaUtil.fromString(input));
        } catch (ClassNotFoundException e) {
            logger.log("Error in deserializing the input String into SQS Message \n");
            e.printStackTrace();
        } catch (IOException e) {
            logger.log("Error in deserializing the input String into SQS Message \n");
            e.printStackTrace();
        }

        logger.log("Lambda worker successfully executed!");
        return "Lambda worker successfully executed!";
    }

    /**
     * Parses the incoming payload, deciphers it and invokes the report rest api
     * calls for CRUD operation in related report optimization tables.
     * 
     * @param message
     *            SQS {@link Message} object containing report service payload
     *            details and service properties
     */
    private void processWorker(Message message) {
        final HttpClient httpClient = HttpClientBuilder.create().build();
        HttpUriRequest uriRequest = null;
        try {
        	logger.log("Message getBody data :::::::::::::"+message.getBody());
            final Map<String, Object> rawMap = JsonUtil.parseMap(message.getBody());
            logger.log("Message rawMap data :::::::::::::"+rawMap);
            final Map<String, Object> map = JsonUtil.parseMap(rawMap.get("rawData").toString());
            logger.log("Message map data :::::::::::::"+map);
            if (null != map && null != map.get("data")) {
                
                SQS_QUEUE_URL = rawMap.get("SQSQueueUrl").toString();
                SQS_REGION = rawMap.get("SQSRegion").toString();
                SQS_QUEUE_NAME = rawMap.get("SQSQueueName").toString();

                // Region needs to be configured in environment variables
                client.setRegion(Region.getRegion(Regions.fromName(SQS_REGION)));
                logger.log("Data class type:::::::::::::::"+map.get("data").getClass());
                Map<String, Object> payLoadMap = (Map<String, Object>) map.get("data");
                if(null != DECRYPTION_NEED){
                	String data = map.get("data").toString(); 		
                	String decryptedData = data;
                	decryptedData = decrypt(ByteBuffer.wrap(Base64.decodeBase64(data)));
                	logger.log("Decrypted base payload is : " + decryptedData + "\n");
                	payLoadMap = JsonUtil.parseMap(decryptedData);
                }
                
                uriRequest = prepareHttpRequest(payLoadMap);
                final HttpResponse response = httpClient.execute(uriRequest);
                if (null != response && response.getStatusLine().getStatusCode() == 200) {
                    logger.log(String.format(
                            ("Message Processed successfully;Deleting message from the SQS queue, queueName = %s & MessageId = %s ; Response Code = %s ; "),
                            SQS_QUEUE_NAME, message.getMessageId(),
                            response.getStatusLine().getStatusCode()) + "\n");
                    deleteSqsMessage(message);
                } else {
                    logger.log(String.format(
                            ("Message Processing failed;Deleting message from the SQS queue, queueName = %s & MessageId = %s ; Response Code = %s ; "),
                            SQS_QUEUE_NAME, message.getMessageId(),
                            response.getStatusLine().getStatusCode()) + "\n");
                }
            }
        } catch (Exception e) {
            logger.log(String.format(
                    ("Message Processing failed;Deleting message from the SQS queue, queueName = %s & MessageId = %s ; Error = %s ; "),
                    SQS_QUEUE_NAME, message.getMessageId(),
                    e.getMessage()) + "\n");
            e.printStackTrace();
        }
    }

    /**
     * <p>
     * After Response success Delete the SQS Message from queue.
     * </p>
     * 
     * @param message
     *            - Need to delete this message from SQS Queue.
     */
    private void deleteSqsMessage(Message message) {
        client.deleteMessage(
                new DeleteMessageRequest().withQueueUrl(SQS_QUEUE_URL).withReceiptHandle(message.getReceiptHandle()));
        logger.log(String.format(("Message is deleted from the SQS queue, queueName = %s & MessageId = %s: "),
                SQS_QUEUE_NAME, message.getMessageId()) + "\n");
    }

    /**
     * <p>
     * Used to prepare HttpUriRequest based on method POST, PUT, DELETE.
     * </p>
     * 
     * @param payLoadMap
     *            - deciphered data for prepare htttRequest
     * @return Return formed HttpUriRequest based on all parameters
     * @throws IOException
     *             - While parsing String to Map
     */
    private HttpUriRequest prepareHttpRequest(Map<String, Object> payLoadMap) throws IOException {
        HttpUriRequest uriRequest = null;
        final Map<String, Object> headers = (Map<String, Object>) payLoadMap.get("headers");
        if (String.valueOf(payLoadMap.get("method")).equals("POST")) {
            uriRequest = prepareHttpPost(payLoadMap, headers);
        } else if (String.valueOf(payLoadMap.get("method")).equals("PUT")) {
            uriRequest = prepareHttpPut(payLoadMap, headers);
        } else if (String.valueOf(payLoadMap.get("method")).equals("DELETE")) {
            uriRequest = new HttpDelete(String.valueOf(payLoadMap.get("url")));
        }
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            uriRequest.setHeader(entry.getKey(), entry.getValue().toString());
        }
        return uriRequest;
    }

    /**
     * <p>
     * Prepare HttpPut method for given httpContent.
     * </p>
     * 
     * @param payLoadMap
     *            - Deciphered data for prepare httpPut request.
     * @param headersMap
     * @return Return formed HttpUriRequest based on all parameters
     * @throws IOException
     *             - While parsing String to Map
     */
    private HttpUriRequest prepareHttpPut(Map<String, Object> payLoadMap, Map<String, Object> headersMap) throws IOException {
        final HttpPut uriRequest = new HttpPut(payLoadMap.get("url").toString());
        if (null != payLoadMap.get("body") && headersMap.get(CONTENT_TYPE) != null) {
            String contentTypeValue = headersMap.get(CONTENT_TYPE).toString();
            if(contentTypeValue.equals(CONTENT_TYPE_APPLICATION_FORM_URLENCODED)) {
                final Map<String, Object> httpbody = (Map<String, Object>) payLoadMap.get("body");
                final ArrayList<NameValuePair> putParameters = new ArrayList<NameValuePair>();
                httpbody.forEach((key, value) -> {
                    putParameters.add(new BasicNameValuePair(key, String.valueOf(value)));
                });
                uriRequest.setEntity(new UrlEncodedFormEntity(putParameters));
            } else if(contentTypeValue.equals(CONTENT_TYPE_APPLICATION_JSON)){
                final JSONObject requestJson = new JSONObject(payLoadMap.get("body").toString());
                uriRequest.setEntity(new StringEntity(requestJson.toString(), ContentType.APPLICATION_JSON));
            }
        }
        return uriRequest;
    }

    /**
     * <p>
     * Prepare HttpPut method for given httpContent.
     * </p>
     * 
     * @param payLoadMap
     *            - Deciphered data for prepare httpPut request.
     * @param headersMap
     * @return Return formed HttpUriRequest based on all parameters
     * @throws IOException
     *             - While parsing String to Map
     */
    private HttpUriRequest prepareHttpPost(Map<String, Object> payLoadMap, Map<String, Object> headersMap) throws IOException {
        final HttpUriRequest uriRequest = new HttpPost(String.valueOf(payLoadMap.get("url")));
        if (null != payLoadMap.get("body") && headersMap.get(CONTENT_TYPE) != null) {
            String contentTypeValue = headersMap.get(CONTENT_TYPE).toString();
            if(contentTypeValue.equals(CONTENT_TYPE_APPLICATION_FORM_URLENCODED)) {
            	final Map<String, Object> httpbody = (Map<String, Object>) payLoadMap.get("body");
                final ArrayList<NameValuePair> postParameters = new ArrayList<NameValuePair>();
                httpbody.forEach((key, value) -> {
                    postParameters.add(new BasicNameValuePair(key, String.valueOf(value)));
                });
                ((HttpPost) uriRequest).setEntity(new UrlEncodedFormEntity(postParameters));
            } else if(contentTypeValue.equals(CONTENT_TYPE_APPLICATION_JSON)){
            	final Map<String, Object> httpbody = (Map<String, Object>) payLoadMap.get("body");
                final JSONObject requestJson = new JSONObject(httpbody);
                ((HttpPost) uriRequest).setEntity(new StringEntity(requestJson.toString(), ContentType.APPLICATION_JSON));
            }
        }
        return uriRequest;
    }

    /**
     * Deciphers the given data using AWS's <code>KMS</code> API.
     * 
     * @param ciphertext
     *            Given ciphertext to decipher
     * 
     * @return String decrypted data
     * @throws IOException
     *             - While parsing String to Map
     */
    public static String decrypt(ByteBuffer ciphertext) throws IOException {
        final AWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);
        final AWSKMSClient client = new AWSKMSClient(credentials);
        client.setRegion(Region.getRegion(Regions.fromName(KEY_REGION)));

        final DecryptRequest request = new DecryptRequest().withCiphertextBlob(ciphertext);
        final ByteBuffer plainText = client.decrypt(request).getPlaintext();

        final String result = new String(plainText.array());
        logger.log("Payload data successfully decrypted for message: " + result + " \n");

        return result;
    }

}
