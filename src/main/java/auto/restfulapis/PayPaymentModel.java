package auto.restfulapis;

import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;

public class PayPaymentModel {
    public String createModel(String email, String bankAccType, String guid, int accNo, long amount, int routingNo, String receivableType, String transType, String agrKey, String paymentMeth, String payorName){

        JSONObject mmpayPaymentBody = new JSONObject();
        mmpayPaymentBody.put("transGuid", guid);
        mmpayPaymentBody.put("processingCode", 1);
        mmpayPaymentBody.put("payorEmailAddress", email);
        mmpayPaymentBody.put("bankAcctType", bankAccType);
        mmpayPaymentBody.put("creatorId", guid);
        mmpayPaymentBody.put("accountNumber", accNo);
        mmpayPaymentBody.put("paymentEffectiveDate", "2024-02-13T18:06:32.468Z");
        mmpayPaymentBody.put("paymentAmount", amount);
        mmpayPaymentBody.put("routingNumber", routingNo);
        mmpayPaymentBody.put("transRequestorId", guid);
        mmpayPaymentBody.put("receivableType", receivableType);
        mmpayPaymentBody.put("transType", transType);
        mmpayPaymentBody.put("agrKey", agrKey);
        mmpayPaymentBody.put("suspensePayment", "false");
        mmpayPaymentBody.put("paymentMethod", paymentMeth);
        mmpayPaymentBody.put("authorizationType", "WEB");
        mmpayPaymentBody.put("creatorSystem", "SS");
        mmpayPaymentBody.put("transExecDateTS", "2024-02-13 01:06:84532 PM");
        mmpayPaymentBody.put("correlationid", "c9afd676-ed75-48b7-b6b1-4fe595236054");
        mmpayPaymentBody.put("customerMemberGuid", "a993f583-0146-e22e-7fff-9d19aa06455f");
        mmpayPaymentBody.put("paymentStatus", "PNDG");
        mmpayPaymentBody.put("payorFullName", payorName);
        return mmpayPaymentBody.toString();
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
