package org.cdlib.mrt.store;

import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.apache.commons.lang3.CharSet;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

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

        public JSONObject addObjectByManifest(String url, String checkm) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                        builder.addBinaryBody(
                          "manifest", new File(checkm), ContentType.APPLICATION_OCTET_STREAM, "file.checkm"
                        );
                        builder.addTextBody("responseForm", "json");
                        HttpEntity multipart = builder.build();
                        post.setEntity(multipart);
                        
                        HttpResponse response = client.execute(post);
                        assertEquals(200, response.getStatusLine().getStatusCode());
    
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertFalse(s.isEmpty());

                        JSONObject json =  new JSONObject(s);
                        assertNotNull(json);
                        assertTrue(json.has("ver:versionState"));
                        return json;
                }

        }

        public void verifyVersion(JSONObject json, String ark, int version, int fileCount) throws JSONException {
                assertTrue(json.has("ver:versionState"));
                JSONObject v = json.getJSONObject("ver:versionState");
                assertTrue(v.getString("ver:version").contains(ark));
                assertEquals(version, v.getInt("ver:identifier"));
                assertEquals(fileCount, v.getInt("ver:numFiles"));
        }

        public void verifyObject(JSONObject json, String ark, int version, int fileCount) throws JSONException {
                assertTrue(json.has("obj:objectState"));
                JSONObject v = json.getJSONObject("obj:objectState");
                assertEquals(ark, v.getString("obj:identifier"));
                assertEquals(version, v.getInt("obj:numVersions"));
                assertEquals(fileCount, v.getInt("obj:numFiles"));
        }

        @Test
        public void AddObjectTest() throws IOException, JSONException, ParserConfigurationException, SAXException {
                String ark = "ark:/1111/2222";
                String ark_enc = URLEncoder.encode(ark, StandardCharsets.UTF_8);
                String url = String.format("http://localhost:%d/%s/add/7777/%s", port, cp, ark_enc);
                String checkm = "src/main/webapp/static/test.checkm";
                JSONObject json = addObjectByManifest(url, checkm);
                verifyVersion(json, ark_enc, 1, 8);

                url = String.format("http://localhost:%d/%s/state/7777/%s?t=json", port, cp, ark_enc);
                json = getJsonContent(url);
                verifyObject(json, ark, 1, 8);

                url = String.format("http://localhost:%d/%s/manifest/7777/%s", port, cp, ark_enc);
                String s = getContent(url);
                DocumentBuilder db = DocumentBuilderFactory.newDefaultInstance().newDocumentBuilder();
                Document d = db.parse(new InputSource(new StringReader(s)));
                assertEquals("objectInfo", d.getDocumentElement().getTagName());
        }

}
