package auto.runners;

import auto.mailboxmonitoring.consumer.ConsumerTest;
import auto.mailboxmonitoring.micro.POSTESendEmailHappyTest;
import auto.mailboxmonitoring.micro_consumer.POSTESendEmailConsumerValidationsHappyTest;
import auto.mailboxmonitoring.micro_consumer.POSTESendEmailConsumerValidationsNegativeTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
        {
                ConsumerTest.class,
                POSTESendEmailHappyTest.class,
                POSTESendEmailConsumerValidationsHappyTest.class,
                POSTESendEmailConsumerValidationsNegativeTest.class
        }
)

public class MailboxTestSuiteRunner {
}
