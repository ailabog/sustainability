package auto.restfulapis;

import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;

public class ESendEmailModel {

    public String createModel(String email, String alias, String recipientEmail, String appID, String phoneNo, String emailBodyValue){

        JSONObject eEmailBody = new JSONObject();

        eEmailBody.put("messageType", "email");

        JSONObject sender = new JSONObject();
        sender.put("replyTo", email);
        sender.put("alias", alias);
        sender.put("email", email);

        eEmailBody.put("sender", sender);
        eEmailBody.put("messageFormat", "text");

        JSONArray recipients = new JSONArray();
        JSONObject objectRecipients = new JSONObject();
        objectRecipients.put("recipientType", "to");
        objectRecipients.put("email", recipientEmail);
        recipients.put(objectRecipients);

        eEmailBody.put("recipients", recipients);

        eEmailBody.put("applicationId", appID);

        JSONObject content = new JSONObject();
        JSONArray items = new JSONArray();
        JSONObject itemsObject1 = new JSONObject();
        itemsObject1.put("templateType", "text");
        itemsObject1.put("index", 0);
        itemsObject1.put("text", phoneNo);
        itemsObject1.put("contentType", "emailSubject");
        items.put(itemsObject1);

        JSONObject itemsObject2 = new JSONObject();
        itemsObject2.put("templateType", "text");
        itemsObject2.put("index", 1);
        itemsObject2.put("text", emailBodyValue);
        itemsObject2.put("contentType", "emailBody");
        items.put(itemsObject2);
        content.put("items", items);
        eEmailBody.put("content", content);

        return eEmailBody.toString();
    }
}
