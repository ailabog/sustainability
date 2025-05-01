package auto.utils.general;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
public class ConfigUtils {
    private static Properties prop = new Properties();
    private static InputStream input = null;

    public enum ConfigKeys {
        BOOTSTREP_SERVER_QA, SCHEMA_REGISTRY, TRUST_STORE_LOCATION, TOPIC_INV_CONSUMER, TOPIC_MAILBOX_CONSUMER,
        AUTH_INFO_INV, SASL_CONFIG_INV, AUTH_INFO_MAILBOX, SASL_CONFIG_MAILBOX,
        TOPIC_NAME_PRODUCER, TOPIC_NAME_PRODUCER_DLQ,
        MMPAY_DB_URL_QA, MMPAY_DB_USERNAME_QA, MMPAY_DB_PASSWORD_QA,
        MIDDLE_URL_INV_QA, END_URL_INV_QA, MIDDLE_URL_INV_MS_QA, END_URL_INV_MS_QA,
        BASE_URL_KONG_QA, MIDDLE_URL_AGREEMENT_QA,  END_URL_AGREEMENT_QA, END_URL_AGREEMENT1_QA,
        E_DB_URL_QA, INV_DB_USERNAME_QA, INV_DB_PASSWORD_QA,
        BASE_URL_QA, MIDDLE_URL_PAYMENT_QA, END_URL_PAYMENT_QA,
        TOKEN_MMPAY_PAYMENT, TOKEN_AGREEMENT_CUSTOMER, TOKEN_CUSTOMER_ID, TOKEN_INV, TOKEN_INV_MS,
        EMAIL_MAILBOX, EMAIL_RECIPIENT_QA, ALIAS_MAILBOX,
        MIDDLE_URL_CUSTOMERID_QA,
        PAYMENT_USERNAME_QA, PAYMENT_PASSWORD_QA;
    }
    public static String getProperty(String propertyKey) {
        String result = "";
        String configFile = System.getProperty("configFile") == null ? "local" : System.getProperty("configFile");
        String fullPath = Constants.CONFIG_RESOURCES_PATH + configFile + "-config.properties";
        try {
            input = new FileInputStream(fullPath);
            prop.load(input);
            result = prop.getProperty(String.valueOf(propertyKey));
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
}