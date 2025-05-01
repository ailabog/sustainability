package auto.restfulapis;

import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;

public class EPushNotifsModel {
    public String createModel(String guid, String originEvDesc, String originAppId, String msgType, String no){

        JSONObject recipientProperties = new JSONObject();
        recipientProperties.put("recipientFieldName", "userId");

        JSONObject recipient = new JSONObject();
        recipient.put("properties", recipientProperties);

        JSONArray recipients = new JSONArray();
        recipients.put(recipient);

        JSONArray onFailureTriggers = new JSONArray();
        onFailureTriggers.put("c-push-notif-failed-event-msg");

        JSONArray onSuccessTriggers= new JSONArray();
        onSuccessTriggers.put("ebill-push-notif-android");
        onSuccessTriggers.put("c-push-notif-success-event-msg");

        JSONObject triggers = new JSONObject();
        triggers.put("onfailure", onFailureTriggers);
        triggers.put("onsuccess", onSuccessTriggers);

        JSONObject sharedData = new JSONObject();
        sharedData.put("origEvId", guid);
        sharedData.put("origEvSDesc", originEvDesc);
        sharedData.put("overwriteMessageType", msgType);
        sharedData.put("origEvInitDesc", originEvDesc);
        sharedData.put("origAppId", originAppId);
        sharedData.put("policyNumber", no);
        sharedData.put("originalEventPublishedDateTime", "2024-04-15T04:46:43.554332253");
        sharedData.put("originalEventSource", "d-comm-orch");
        sharedData.put("originalEventRequestID", guid);
        sharedData.put("originalEventInitiator", originAppId);
        sharedData.put("userId", "0cb60577-8f17-4c2f-b676-28181e337175");
        sharedData.put("originalEventDateTime", "2024-04-15 11:46:39");

        JSONObject sourceParameters = new JSONObject();
        sourceParameters.put("fileName", "inv-push-notif-template");

        JSONObject source = new JSONObject();
        source.put("sourceType", "aurora");
        source.put("parameters", sourceParameters);

        JSONObject item = new JSONObject();
        item.put("templateType", "thymeleaf");
        item.put("index", 0);
        item.put("source", source);
        item.put("contentType", "pNotifTemplate");

        JSONArray items = new JSONArray();
        items.put(item);

        JSONObject content = new JSONObject();
        content.put("sharedData", sharedData);
        content.put("items", items);

        JSONObject ecorrPayload = new JSONObject();
        ecorrPayload.put("messageType", msgType);
        ecorrPayload.put("messageFormat", "TEXT");
        ecorrPayload.put("recipients",  recipients);
        ecorrPayload.put("ident", 0);
        ecorrPayload.put("applicationId", originAppId);
        ecorrPayload.put("triggers", triggers);
        ecorrPayload.put("content", content);
        ecorrPayload.put("attempts", 0);
        return ecorrPayload.toString();
    }
    public int savePaymentKey(Response responseBody_payment){
        JSONObject jsonObject = new JSONObject(responseBody_payment.asString());
        JSONArray msgArray = jsonObject.getJSONArray("messages");
        JSONObject msgObject = msgArray.getJSONObject(0);
        String msgDesc =msgObject.getString("msgDesc");
        String loankeyString = msgDesc.substring(msgDesc.lastIndexOf(" " ) + 1);
        int paymentKey = Integer.parseInt(loankeyString.trim());
        return paymentKey;
    }
}
