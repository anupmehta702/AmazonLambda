package com.anup;


import com.amazonaws.services.dynamodbv2.model.Record;
import com.anup.model.Customer;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/*
    INSERT
    {"awsRegion":"us-east-2","dynamodb":{
    "ApproximateCreationDateTime":1614669851662,
    "Keys":{"id":{"S":"3"},"name":{"S":"ABC"}},
    "NewImage":{"email":{"S":"abc@email.com"},"id":{"S":"3"},"name":{"S":"ABC"}},"SizeBytes":38},
    "eventID":"273538ad-fc6e-453c-90d5-999d2399375d",
    "eventName":"INSERT","userIdentity":null,"recordFormat":"application/json",
    "tableName":"customer","eventSource":"aws:dynamodb"}
    UPDATE
    {"awsRegion":"us-east-2","dynamodb":{"ApproximateCreationDateTime":1614669922526,
    "Keys":{"id":{"S":"3"},"name":{"S":"ABC"}},
    "NewImage":{"email":{"S":"abc123@email.com"},"id":{"S":"3"},"name":{"S":"ABC"}},
    "OldImage":{"email":{"S":"abc@email.com"},"id":{"S":"3"},"name":{"S":"ABC"}},"SizeBytes":69},
    "eventID":"b2945a3e-239e-4c49-8978-bb96c843bee8","eventName":"MODIFY","userIdentity":null,
    "recordFormat":"application/json",
    "tableName":"customer","eventSource":"aws:dynamodb"}
    DELETE
    {"awsRegion":"us-east-2","dynamodb":{"ApproximateCreationDateTime":1614685262643,
    "Keys":{"id":{"S":"2"},"name":{"S":"Anoop"}},
    "OldImage":{"email":{"S":"anoop@email.com"},"id":{"S":"2"},"name":{"S":"Anoop"}},"SizeBytes":44}
    ,"eventID":"b8d3bbb3-3971-4331-ae2a-d095f842da38","eventName":"REMOVE","userIdentity":null
    ,"recordFormat":"application/json","tableName":"customer","eventSource":"aws:dynamodb"}
     */
public class CDCSerializer {
    private static ObjectMapper JSON = new ObjectMapper();

    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //Need to add below mixin bcoz Record and StreamRecord have got two setter overloaded methods and Jackson throws
        //conflicting setter definition error
        JSON.addMixIn(com.amazonaws.services.dynamodbv2.model.Record.class, RecordIgnoreDuplicateMethods.class);
        JSON.addMixIn(com.amazonaws.services.dynamodbv2.model.StreamRecord.class, StreamRecordIgnoreDuplicateMethods.class);
    }

    public static Customer serializeIntoCustomer(String data) throws IOException {
        //Need to do below step to ensure json transformation into object happens correctly
        String newData = data.replace("ApproximateCreationDateTime", "approximateCreationDateTime")
                .replace("Keys", "keys")
                .replace("NewImage", "newImage")
                .replace("OldImage", "oldImage")
                .replace("SizeBytes", "sizeBytes")
                .replace("\"S\"", "\"s\"");
        Record record = JSON.readValue(newData, Record.class);
        System.out.println("Serializing record of type - " + record.getEventName());
        return Customer.setData(record.getDynamodb());
    }

    public static void main(String[] args) throws IOException {
        String data = " {\"awsRegion\":\"us-east-2\",\"dynamodb\":{\"ApproximateCreationDateTime\":1614669922526,\n" +
                "    \"Keys\":{\"id\":{\"S\":\"3\"},\"name\":{\"S\":\"ABC\"}},\n" +
                "    \"NewImage\":{\"email\":{\"S\":\"abc123@email.com\"},\"id\":{\"S\":\"3\"},\"name\":{\"S\":\"ABC\"}},\n" +
                "    \"OldImage\":{\"email\":{\"S\":\"abc@email.com\"},\"id\":{\"S\":\"3\"},\"name\":{\"S\":\"ABC\"}},\"SizeBytes\":69},\n" +
                "    \"eventID\":\"b2945a3e-239e-4c49-8978-bb96c843bee8\",\"eventName\":\"MODIFY\",\"userIdentity\":null,\n" +
                "    \"recordFormat\":\"application/json\",\n" +
                "    \"tableName\":\"customer\",\"eventSource\":\"aws:dynamodb\"}";
        System.out.println("Customer data --> " + CDCSerializer.serializeIntoCustomer(data));

    }
}
