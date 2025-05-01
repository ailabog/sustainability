package auto.runners;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
        {
                auto.Pay.POSTPayPaymentPayPaymentStagingHistoryDBValidationsTest.class
        }
)
public class PayTestSuiteRunner {
}
