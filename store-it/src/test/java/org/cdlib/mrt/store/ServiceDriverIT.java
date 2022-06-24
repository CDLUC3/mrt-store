package org.cdlib.mrt.store;

import org.junit.Test;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.Writer;
import java.io.IOException;
import static org.junit.Assert.*;

public class ServiceDriverIT {
        private int port = 8080;
        private String cp = "store";

        public ServiceDriverIT() {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port not set");
                }
        }

        public String getContent(String url) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    assertEquals(200, response.getStatusLine().getStatusCode());

                    String s = new BasicResponseHandler().handleResponse(response).trim();
                    assertFalse(s.isEmpty());
                    return s;
                }
        }

        public JSONObject getJsonContent(String url) throws HttpResponseException, IOException, JSONException {
                String s = getContent(url);
                JSONObject json =  new JSONObject(s);
                assertNotNull(json);
                return json;
        }

        @Test
        public void SimpleTest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                JSONObject json = getJsonContent(url);
                assertTrue(json.has("sto:storageServiceState"));
        }

        @Test
        public void SimpleTest7777() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state/7777?t=json", port, cp);
                JSONObject json = getJsonContent(url);
                assertTrue(json.has("nod:nodeState"));
        }

}
