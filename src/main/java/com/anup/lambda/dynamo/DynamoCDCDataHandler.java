package com.anup.lambda.dynamo;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsInputPreprocessingResponse;
import com.anup.model.Customer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


// Handler value: com.anup.lambda.dynamo.DynamoCDCDataHandler
public class DynamoCDCDataHandler implements RequestHandler<DynamodbEvent, KinesisAnalyticsInputPreprocessingResponse> {

    private static List<KinesisAnalyticsInputPreprocessingResponse.Record> records = new ArrayList<>();
    private static KinesisAnalyticsInputPreprocessingResponse responseObj = new KinesisAnalyticsInputPreprocessingResponse();
    @Override
    public KinesisAnalyticsInputPreprocessingResponse handleRequest(DynamodbEvent dynamodbEvent, Context context) {
        {
            LambdaLogger logger = context.getLogger();
            logger.log("In handler to handle DynamoDB CDC event for --> "+dynamodbEvent);
            for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {
                logger.log("EventId -->" + record.getEventID());
                logger.log("EventName -->" + record.getEventName());
                logger.log("DynamoDB value -->" + record.getDynamodb().toString());
                Customer customer = Customer.setData(record.getDynamodb());
                logger.log("Processed data -->" + customer);
                convertDataToKinesisAnalyticsRecord(customer);
                /*try {
                    CSVWriter.writeToCSVFile(customer);
                    BucketOperation.putObject();
                } catch (IOException e) {
                    e.printStackTrace();
                }*/

            }
            responseObj.setRecords(records);
            logger.log("Successfully processed " + dynamodbEvent.getRecords().size() + " records.");

            return responseObj;
        }
    }

    private static void convertDataToKinesisAnalyticsRecord(Customer customer){
        KinesisAnalyticsInputPreprocessingResponse.Record rec = new KinesisAnalyticsInputPreprocessingResponse.Record();
        rec.setData(ByteBuffer.wrap(customer.toJsonAsBytes()));
        rec.setResult(KinesisAnalyticsInputPreprocessingResponse.Result.Ok);
        rec.setRecordId(customer.getId());
        records.add(rec);
    }

}

/*
Test event data --
{
  "Records": [
    {
      "eventID": "1",
      "eventVersion": "1.0",
      "dynamodb": {
        "Keys":{"id":{"S":"3"},"name":{"S":"ABC"}},
        "NewImage": {"email":{"S":"abc@email.com"},"id":{"S":"3"},"name":{"S":"ABC"}},
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "111",
        "SizeBytes": 26
      },
      "awsRegion": "us-east-2",
      "eventName": "INSERT",
      "eventSource": "aws:dynamodb",
	  "tableName":"customer",
	  "recordFormat":"application/json"
    }

  ]
}
 */

