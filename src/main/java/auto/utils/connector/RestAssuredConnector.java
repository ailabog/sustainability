package auto.utils.connector;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;

public class RestAssuredConnector {
    public RequestSpecification request = new RequestSpecBuilder().build();
    public static Response response;

    public static void main(String[] args) {
        RestAssuredConnector rac = new RestAssuredConnector();
        System.out.println(response.asString());
    }

    public Response postRequest(String uri, Map<String, String> headers, String body) {
        request = given().baseUri(uri).body(body).headers(headers);
        return request.when().post();
    }

    public Response getRequest(String uri, Map<String, String> headers) {
        request = given().baseUri(uri).headers(headers);
        return request.when().get();
    }

    public Response deleteRequest(String uri, String host, Map<String, String> headers) {
        request = given().baseUri(uri).proxy(host).headers(headers);
        return request.when().delete();
    }

    public static Map<String, String> setHeaders(String token) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "*/*");
        headers.put("Authorization", token);
        return headers;
    }
}
