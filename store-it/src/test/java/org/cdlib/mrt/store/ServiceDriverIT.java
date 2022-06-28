package org.cdlib.mrt.store;

import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import javax.xml.xpath.XPathFactory;
//https://stackoverflow.com/a/22939742/3846548
import org.apache.xpath.jaxp.XPathFactoryImpl;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;

import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.*;

public class ServiceDriverIT {
        private int port = 8080;
        private String cp = "store";
        private DocumentBuilder db;
        private XPathFactory xpathfactory;

        public ServiceDriverIT() throws ParserConfigurationException {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port not set, defaulting to " + port);
                }
                db = DocumentBuilderFactory.newDefaultInstance().newDocumentBuilder();
                xpathfactory = new XPathFactoryImpl();
        }

        public String getContent(String url, int status) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    assertEquals(status, response.getStatusLine().getStatusCode());

                    if (status > 300) {
                        return "";
                    }
                    String s = new BasicResponseHandler().handleResponse(response).trim();
                    assertFalse(s.isEmpty());
                    return s;
                }
        }

        public JSONObject getJsonContent(String url, int status) throws HttpResponseException, IOException, JSONException {
                String s = getContent(url, status);
                JSONObject json =  new JSONObject(s);
                assertNotNull(json);
                return json;
        }

        @Test
        public void SimpleTest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                JSONObject json = getJsonContent(url, 200);
                assertTrue(json.has("sto:storageServiceState"));
        }

        @Test
        public void SimpleTest7777() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state/7777?t=json", port, cp);
                JSONObject json = getJsonContent(url, 200);
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

        public JSONObject deleteObject(String url) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpDelete del = new HttpDelete(url);
                       
                        HttpResponse response = client.execute(del);
                        assertEquals(200, response.getStatusLine().getStatusCode());
    
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertFalse(s.isEmpty());

                        JSONObject json =  new JSONObject(s);
                        return json;
                }

        }


        public void verifyVersion(JSONObject json, String ark, int version, int fileCount) throws JSONException {
                assertTrue(json.has("ver:versionState"));
                JSONObject v = json.getJSONObject("ver:versionState");
                assertTrue(v.getString("ver:version").contains(URLEncoder.encode(ark, StandardCharsets.UTF_8)));
                assertEquals(version, v.getInt("ver:identifier"));
                assertEquals(fileCount, v.getInt("ver:numFiles"));
        }

        public void verifyObject(JSONObject json, String ark, int version, int fileCount) throws JSONException {
                assertTrue(json.has("obj:objectState"));
                JSONObject v = json.getJSONObject("obj:objectState");
                assertEquals(ark, v.getString("obj:identifier"));
                assertEquals(version, v.getInt("obj:numVersions"));
                assertEquals(fileCount, v.getInt("obj:numActualFiles"));
        }

        public void verifyFile(JSONObject json, String path, int size) throws JSONException {
                assertTrue(json.has("fil:fileState"));
                JSONObject v = json.getJSONObject("fil:fileState");
                assertEquals(path, v.getString("fil:identifier"));
                assertEquals(size, v.getInt("fil:size"));
        }

        public void verifyObjectInfo(Document d, String ark, int version, int fileCount) throws JSONException, XPathExpressionException {
                XPath xpath = xpathfactory.newXPath();
                XPathExpression expr = xpath.compile("/objectInfo/object/@id");
                String result = expr.evaluate(d, XPathConstants.STRING).toString();
                assertEquals(ark, result);
                expr = xpath.compile("/objectInfo/object/current/text()");
                result = expr.evaluate(d, XPathConstants.STRING).toString();
                assertEquals(version, Integer.parseInt(result));
                expr = xpath.compile("/objectInfo/object/actualCount/text()");
                result = expr.evaluate(d, XPathConstants.STRING).toString();
                assertEquals(fileCount, Integer.parseInt(result));
        }

        public Document getDocument(String body, String tag) throws SAXException, IOException {
                Document d = db.parse(new InputSource(new StringReader(body)));
                assertEquals(tag, d.getDocumentElement().getTagName());
                return d;
        }

        public String addUrl(String ark) {
                return String.format(
                        "http://localhost:%d/%s/add/7777/%s", 
                        port, 
                        cp, 
                        URLEncoder.encode(ark, StandardCharsets.UTF_8)
                );
        }

        public String updateUrl(String ark) {
                return String.format(
                        "http://localhost:%d/%s/update/7777/%s", 
                        port, 
                        cp, 
                        URLEncoder.encode(ark, StandardCharsets.UTF_8)
                );
        }

        public String deleteUrl(String ark) {
                return String.format(
                        "http://localhost:%d/%s/content/7777/%s?t=json",
                        port, 
                        cp, 
                        URLEncoder.encode(ark, StandardCharsets.UTF_8)
                );
         }

        public String stateUrl(String ark) {
                return String.format(
                        "http://localhost:%d/%s/state/7777/%s?t=json", 
                        port, 
                        cp, 
                        URLEncoder.encode(ark, StandardCharsets.UTF_8)
                );
        }

        public String stateUrl(String ark, int version) {
                return String.format(
                        "http://localhost:%d/%s/state/7777/%s/%d?t=json", 
                        port, 
                        cp, 
                        URLEncoder.encode(ark, StandardCharsets.UTF_8),
                        version
                );
        }

        public String stateUrl(String ark, int version, String path) {
                return String.format(
                        "http://localhost:%d/%s/state/7777/%s/%d/%s?t=json", 
                        port, 
                        cp, 
                        URLEncoder.encode(ark, StandardCharsets.UTF_8),
                        version,
                        URLEncoder.encode(path, StandardCharsets.UTF_8)
                );
        }

        public String manifestUrl(String ark) {
                return String.format(
                        "http://localhost:%d/%s/manifest/7777/%s", 
                        port, 
                        cp,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8)
                );
        }

        @Test
        public void AddObjectTest() throws IOException, JSONException, ParserConfigurationException, SAXException, XPathExpressionException {
                String ark = "ark:/1111/2222";
                String checkm = "src/main/webapp/static/object1/test.checkm";

                try {
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        verifyVersion(json, ark, 1, 8);
        
                        json = getJsonContent(stateUrl(ark), 200);
                        verifyObject(json, ark, 1, 8);
        
                        json = getJsonContent(stateUrl(ark, 1), 200);
                        verifyVersion(json, ark, 1, 8);
        
                        String path = "producer/hello.txt";
                        json = getJsonContent(stateUrl(ark, 1, path), 200);
                        verifyFile(json, path, 5);

                        String s = getContent(manifestUrl(ark), 200);
                        Document d = getDocument(s, "objectInfo");
                        verifyObjectInfo(d, ark, 1, 8);        
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);        
                }
        }

        @Test
        public void AddObjectAndVerTest() throws IOException, JSONException, ParserConfigurationException, SAXException, XPathExpressionException {
                String ark = "ark:/1111/3333";
                String checkm = "src/main/webapp/static/object1/test.checkm";
                String checkmv2 = "src/main/webapp/static/object1v2/test.checkm";

                try {
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        verifyVersion(json, ark, 1, 8);
        
                        json = addObjectByManifest(updateUrl(ark), checkmv2);
                        verifyVersion(json, ark, 2, 9);

                        json = getJsonContent(stateUrl(ark), 200);
                        verifyObject(json, ark, 2, 9);
        
                        json = getJsonContent(stateUrl(ark, 2), 200);
                        verifyVersion(json, ark, 2, 9);
        
                        String path = "producer/hello.txt";
                        json = getJsonContent(stateUrl(ark, 1, path), 200);
                        verifyFile(json, path, 5);

                        path = "producer/hello2.txt";
                        json = getJsonContent(stateUrl(ark, 2, path), 200);
                        verifyFile(json, path, 5);

                        String s = getContent(manifestUrl(ark), 200);
                        Document d = getDocument(s, "objectInfo");
                        verifyObjectInfo(d, ark, 2, 9);        
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);        
                }
        }

}
