package com.anup.lambda.dynamo;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsInputPreprocessingResponse;
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent;
import com.anup.CDCSerializer;
import com.anup.model.Customer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

//com.anup.lambda.dynamo.KinesisFirehoseDataHandler::handleRequest
public class KinesisFirehoseDataHandler implements RequestHandler<KinesisFirehoseEvent, KinesisAnalyticsInputPreprocessingResponse> {
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private static List<KinesisAnalyticsInputPreprocessingResponse.Record> records = new ArrayList<>();
    private static KinesisAnalyticsInputPreprocessingResponse responseObj = new KinesisAnalyticsInputPreprocessingResponse();

    @Override
    public KinesisAnalyticsInputPreprocessingResponse handleRequest(KinesisFirehoseEvent event, Context context) {

        LambdaLogger logger = context.getLogger();
        logger.log("In KinesisFirehoseDataHandler for data --> " + event);
        for (KinesisFirehoseEvent.Record rec : event.getRecords()) {
            logger.log("Printing data -->" + new String(rec.getData().array()));
            convertDataToKinesisAnalyticsRecord(rec);
        }
        responseObj.setRecords(records);
        return responseObj;

    }

    private static void convertDataToKinesisAnalyticsRecord(KinesisFirehoseEvent.Record record) {
        KinesisAnalyticsInputPreprocessingResponse.Record rec = new KinesisAnalyticsInputPreprocessingResponse.Record();
        try {
            Customer customer = CDCSerializer.serializeIntoCustomer(new String(record.getData().array()));
            rec.setData(ByteBuffer.wrap(customer.toJsonAsBytes()));
            rec.setResult(KinesisAnalyticsInputPreprocessingResponse.Result.Ok);
            rec.setRecordId(record.getRecordId());

        } catch (IOException e) {
            e.printStackTrace();
            rec.setResult(KinesisAnalyticsInputPreprocessingResponse.Result.ProcessingFailed);
            rec.setRecordId(record.getRecordId());
        }
        records.add(rec);
    }
}
