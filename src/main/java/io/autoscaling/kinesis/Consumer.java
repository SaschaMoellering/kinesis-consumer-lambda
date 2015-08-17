package io.autoscaling.kinesis;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import io.autoscaling.proto.AddressBookProtos;

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

            AddressBookProtos.AddressBook.Builder addressBookBuilder = AddressBookProtos.AddressBook.newBuilder();
            addressBookBuilder.mergeFrom(data.array());
            AddressBookProtos.AddressBook addressBook = addressBookBuilder.build();

            logger.log(addressBook.toString());
        }
    }

}
