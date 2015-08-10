package io.autoscaling.kinesis;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

/**
 * Created by sascha.moellering on 07/07/2015.
 */
public class Consumer {

    /**
     * Kinesis record handler
     *
     * @param event The Kinesis event
     * @throws IOException If an exception occurs
     */
    public void recordHandler(KinesisEvent event, Context context) throws IOException {
        LambdaLogger logger = context.getLogger();
        for(KinesisEventRecord rec : event.getRecords()) {
            ByteBuffer data = rec.getKinesis().getData();
            String logEntry = new String(data.array(), "UTF-8");
            logger.log(logEntry);
        }
    }

}
