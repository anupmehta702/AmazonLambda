package com.anup.lambda.dynamo;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsInputPreprocessingResponse;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.anup.model.Customer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;


// Handler value: com.anup.lambda.dynamo.KinesisDataHandler::handleRequest
public class KinesisDataHandler implements RequestHandler<KinesisEvent, KinesisAnalyticsInputPreprocessingResponse> {

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private static List<KinesisAnalyticsInputPreprocessingResponse.Record> records = new ArrayList<>();
    private static KinesisAnalyticsInputPreprocessingResponse responseObj = new KinesisAnalyticsInputPreprocessingResponse();

    @Override
    public KinesisAnalyticsInputPreprocessingResponse handleRequest(KinesisEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("In KinesisDataHandler for data --> "+event);
        for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
            System.out.println("Printing data -->"+new String(rec.getKinesis().getData().array()));
            logger.log("Printing data -->"+new String(rec.getKinesis().getData().array()));
            convertDataToKinesisAnalyticsRecord(rec);
        }
        responseObj.setRecords(records);
        return responseObj;
    }

    private static void convertDataToKinesisAnalyticsRecord(KinesisEvent.KinesisEventRecord record){
        KinesisAnalyticsInputPreprocessingResponse.Record rec = new KinesisAnalyticsInputPreprocessingResponse.Record();
        rec.setData(ByteBuffer.wrap(record.getKinesis().getData().array()));
        rec.setResult(KinesisAnalyticsInputPreprocessingResponse.Result.Ok);
        rec.setRecordId("randomId");
        records.add(rec);
    }

}


