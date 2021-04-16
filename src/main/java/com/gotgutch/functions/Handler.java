package com.gotgutch.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Handler value: com.gotgutch.functions.Handler
public class Handler implements RequestHandler<S3Event, String> {

  Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger logger = LoggerFactory.getLogger(Handler.class);
  private final String RTF_TYPE = (String) "rtf";
  private static final String QUEUE_NAME = "dev-bgutch-s2f-task-queue";

  @Override
  public String handleRequest(S3Event s3event, Context context) {

      logger.info("EVENT: " + gson.toJson(s3event));
      S3EventNotificationRecord record = s3event.getRecords().get(0);
      
      String srcBucket = record.getS3().getBucket().getName();

      // Object key may have spaces or unicode non-ASCII characters.
      String srcKey = record.getS3().getObject().getUrlDecodedKey();

      String dstBucket = srcBucket;
      String messageBody = "s2f-soa-file:"+ srcBucket + "/" + srcKey;

      // Infer the doc type.
      Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
      if (!matcher.matches()) {
          logger.info("Unable to infer object type for key " + srcKey);
          return "";
      }
      String fileType = matcher.group(1);
      if (!(RTF_TYPE.equals(fileType))) {
          logger.info("Skipping non-rtf " + srcKey);
          return "";
      }

      // Setup the sqs client with the correct queue url
      AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
      String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
      // Create the message to process this object
      SendMessageRequest send_msg_request = new SendMessageRequest()
              .withQueueUrl(queueUrl)
              .withMessageBody(messageBody)
              .withMessageAttributes(getMessageAttributes(srcBucket, srcKey, dstBucket))
              .withDelaySeconds(5);

      // Send the message
      logger.info("Sending message to: " + queueUrl);
      try {
        sqs.sendMessage(send_msg_request);
      }
      catch(AmazonServiceException e)
      {
        logger.error(e.getErrorMessage());
        System.exit(1);
      }
      logger.info("Successfully queued message for processing s3://" + srcBucket + "/" + srcKey);

      return "Ok";
  }

    /**
     * This method will create the correct message attributes for the message to be sent
     * for each object we need to process
     *
     * @param srcBucket The bucket name
     * @param srcKey The object key of the data
     * @return A Map of MessageAttributes
     */
  private Map<String, MessageAttributeValue> getMessageAttributes(String srcBucket, String srcKey, String dstBucket) {
    Map<String, MessageAttributeValue> msgAttributes = new HashMap<String, MessageAttributeValue>();
    MessageAttributeValue inputObject = new MessageAttributeValue();
    inputObject.setDataType("string");
    inputObject.setStringValue("s3://" + srcBucket + "/" + srcKey);
    MessageAttributeValue outputPrefix = new MessageAttributeValue();
    outputPrefix.setDataType("string");
    outputPrefix.setStringValue("s3://" + dstBucket + "/" + srcKey + ".OUT/");
    msgAttributes.put("INPUT_OBJECT",inputObject);
    msgAttributes.put("OUTPUT_PREFIX",outputPrefix);
    return null;
  }
}