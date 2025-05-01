package auto.runners;

import auto.inv.consumer.ConsumerTest;
import auto.inv.micros.GETCstIdHappyTest;
import auto.inv.micros.POSTEPNotifsHappyTest;
import auto.inv.micros_db.POSTEPNotifsDBValidationsTest;
import auto.inv.producer.*;
import auto.inv.producer_consumer.ProducerAvroSchemaConsumerValidationsHappyTest;
import auto.inv.producer_consumer.ProducerAvroSchemaDummyDataConsumerValidationsTest;
import auto.inv.producer_consumer.ProducerAvroSchemaInvDocTypeConsumerValidationsTest;
import auto.inv.producer_consumer_micros_db.ProducerConsumerAgrMsDBValidationsHappyTest;
import auto.inv.producer_db.*;
import auto.inv.producer_micros_db.ProducerAvroSchemaMicrosDBValidationsTypePushHappyTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
        {
                /*  Happy tests */
                ConsumerTest.class,
                ProducerAvroSchemaHappyTest.class,
                ProducerAvroSchemaConsumerValidationsHappyTest.class,
                ProducerConsumerAgrMsDBValidationsHappyTest.class,
                ProducerAvroSchemaDBValidationsTypePAndroidHappyTest.class,
                ProducerMessageStatusesDBValidationsHappyTest.class,
                ProducerAvroSchemaMicrosDBValidationsTypePushHappyTest.class,

                /* Unhappy tests */
                ProducerAvroSchemaInvDocTypeNegativeTest.class,
                ProducerSendNullMessageNegativeTest.class,
                ProducerAvroSchemaDummyDataTest.class,
                ProducerAvroSchemaInvDocTypeConsumerValidationsTest.class,
                ProducerAvroSchemaDummyDataConsumerValidationsTest.class,
                ProducerAvroSchemaInvDocTypeDBValidationsNegativeTest.class,
                ProducerMessageStatusesDBValidationsNegativeTest.class,
                ProducerAvroSchemaDBValidationsTypePAndroidNegativeTest.class,

                /* Micros */
                massmutual.ebill.micros.GETAgrCstHappyTest.class,
                GETCstIdHappyTest.class,
                POSTEPNotifsHappyTest.class,
                POSTEPNotifsDBValidationsTest.class,

                /* DQL tests */
                DLQProducerMessageHappyTest.class,
                DLQProducerSendDummyDataTest.class,
                DLQProducerSendNullMessageNegativeTest.class

        }
)
public class InvPNotifsTestSuiteRunner {
}
