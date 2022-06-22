package org.cdlib.mrt.store;

import org.junit.Test;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.File;
import java.io.Writer;
import java.io.IOException;
import static org.junit.Assert.*;

public class ServiceDriverIT {
        private int port = 8080;
        private String cp = "/store";

        public ServiceDriverIT() {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port not set");
                }
        }

        public String getContent(String url) {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);

                    return new BasicResponseHandler().handleResponse(response).trim();
                } catch(Exception e) {
                    System.err.println(e.getMessage());
                }
                return "";
        }

        @Test
        public void SimpleTest() throws IOException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                String resp = "";
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpGet request = new HttpGet(url);
                        HttpResponse response = client.execute(request);
                        assertEquals(200, response.getStatusLine().getStatusCode());
                        resp = new BasicResponseHandler().handleResponse(response).trim();
                }
                
                System.out.println(resp);
                assertFalse(resp.isEmpty());
        }

        @Test
        public void SimpleTest7777() throws IOException {
                String url = String.format("http://localhost:%d/%s/state/7777?t=json", port, cp);
                String resp = "";
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpGet request = new HttpGet(url);
                        HttpResponse response = client.execute(request);
                        assertEquals(200, response.getStatusLine().getStatusCode());
                        resp = new BasicResponseHandler().handleResponse(response).trim();
                }
                
                System.out.println(resp);
                assertFalse(resp.isEmpty());
        }

}
