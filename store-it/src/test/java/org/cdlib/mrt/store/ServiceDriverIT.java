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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;

public class ServiceDriverIT {
        private int port = 8080;
        private int node = 7777;
        private int ASSM_WAIT = 1500;
        private int ASSM_TRIES = 16;
        private String cp = "store";
        private DocumentBuilder db;
        private XPathFactory xpathfactory;

        public ServiceDriverIT() throws ParserConfigurationException {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port not set, defaulting to " + port);
                }
                db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                xpathfactory = new XPathFactoryImpl();
        }

        public String getContent(String url, int status) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    if (status > 0) {
                        assertEquals(status, response.getStatusLine().getStatusCode());
                    }

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

        public List<String> getZipContent(String url, int status) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    assertEquals(status, response.getStatusLine().getStatusCode());

                    List<String> entries = new ArrayList<>();
                    if (status < 300) {
                            try(ZipInputStream zis = new ZipInputStream(response.getEntity().getContent())){
                                    for(ZipEntry ze = zis.getNextEntry(); ze != null; ze = zis.getNextEntry()) {
                                            entries.add(ze.getName());
                                    }
                            }
                    }

                    return entries;
                }
        }


        @Test
        public void SimpleTest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                JSONObject json = getJsonContent(url, 200);
                assertTrue(json.has("sto:storageServiceState"));
        }

        @Test
        public void SimpleTest7777() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state/%d?t=json", port, cp, node);
                JSONObject json = getJsonContent(url, 200);
                assertTrue(json.has("nod:nodeState"));
        }

        @Test
        public void testJsonState() throws HttpResponseException, IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/jsonstate", port, cp);
                testNodeStatus(getJsonContent(url, 200), "NodesState");
        }

        @Test
        public void testJsonStatus() throws HttpResponseException, IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/jsonstatus", port, cp);
                testNodeStatus(getJsonContent(url, 200), "NodesStatus");
        }

        public void testNodeStatus(JSONObject json, String key) throws HttpResponseException, IOException, JSONException {
                JSONArray jarr = json.getJSONArray(key);
                assertTrue(jarr.length() > 0);
                for(int i=0; i < jarr.length(); i++){
                        assertTrue(jarr.getJSONObject(i).getBoolean("running"));
                }
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

        public JSONObject copyObject(String url) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        
                        HttpResponse response = client.execute(post);
                        assertEquals(200, response.getStatusLine().getStatusCode());
    
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertFalse(s.isEmpty());

                        JSONObject json =  new JSONObject(s);
                        assertNotNull(json);
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


        public void verifyVersion(JSONObject json, String ark, int version, int fileCount) throws JSONException, UnsupportedEncodingException {
                assertTrue(json.has("ver:versionState"));
                JSONObject v = json.getJSONObject("ver:versionState");
                assertTrue(v.getString("ver:version").contains(URLEncoder.encode(ark, StandardCharsets.UTF_8.name())));
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

        public void verifyObjectFixity(JSONObject json) throws JSONException {
                assertTrue(json.has("ofix:objectFixityState"));
                JSONObject v = json.getJSONObject("ofix:objectFixityState");
                assertTrue(v.getBoolean("ofix:match"));
        }

        public void verifyFile(JSONObject json, String path, int size) throws JSONException {
                assertTrue(json.has("fil:fileState"));
                JSONObject v = json.getJSONObject("fil:fileState");
                assertEquals(path, v.getString("fil:identifier"));
                assertEquals(size, v.getInt("fil:size"));
        }

        public void verifyFileFixity(JSONObject json, String hash) throws JSONException {
                assertTrue(json.has("fix:fileFixityState"));
                JSONObject v = json.getJSONObject("fix:fileFixityState");
                assertEquals(hash, v.getString("fix:fileDigest"));
                JSONArray arr = v.getJSONArray("fix:sizeMatches");
                for(int i=0; i<arr.length(); i++) {
                        assertTrue(arr.getBoolean(i));
                }
                arr = v.getJSONArray("fix:digestMatches");
                for(int i=0; i<arr.length(); i++) {
                        assertTrue(arr.getBoolean(i));
                }
        }

        public void verifyPresignFile(JSONObject json) throws JSONException {
                assertEquals("Presigned URL created", json.get("message"));
                assertTrue(json.has("expires"));
                assertTrue(json.has("url"));
                assertEquals(200, json.getInt("status"));
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

        public void verifyVersionLink(JSONObject json, String ark, int fileCount) throws JSONException {
                assertEquals(ark, json.get("objectID"));
                assertEquals(fileCount, json.getInt("versionFileCnt"));
        }

        public void verifyIngestLink(String s, int fileCount) {
                int files = 0;
                for(String row: s.split("\n")) {
                        if (row.matches("^http.*")) {
                                files++;
                        }
                }
                assertEquals(fileCount, files);
        }

        public Document getDocument(String body, String tag) throws SAXException, IOException {
                Document d = db.parse(new InputSource(new StringReader(body)));
                assertEquals(tag, d.getDocumentElement().getTagName());
                return d;
        }

        public String addUrl(String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/add/%d/%s", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String copyUrl(String ark, int copynode) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/copy/%d/%d/%s?t=json", 
                        port, 
                        cp, 
                        node,
                        copynode,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String updateUrl(String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/update/%d/%s", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String accessUrl() throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/flag/set/access?t=json", 
                        port, 
                        cp, 
                        node
                );
        }

        public String lockUrl() throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/flag/set/access/LargeAccessHold?t=json", 
                        port, 
                        cp, 
                        node
                );
        }

        public String unlockUrl() throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/flag/clear/access/LargeAccessHold?t=json", 
                        port, 
                        cp, 
                        node
                );
        }


        public String deleteUrl(String ark) throws UnsupportedEncodingException {
                return deleteUrl(node, ark);
        }

        public String deleteUrl(int curnode, String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/content/%d/%s?t=json",
                        port, 
                        cp, 
                        curnode,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
         }

        public String nodeStateUrl(int curnode, String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/state/%d/%s?t=json", 
                        port, 
                        cp, 
                        curnode,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String stateUrl(String ark) throws UnsupportedEncodingException {
                return nodeStateUrl(node, ark);
        }

        public String fixityUrl(String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/fixity/%d/%s?t=json", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String downloadObjectUrl(String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/content/%d/%s?t=zip&X=false", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String downloadProducerUrl(String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/producer/%d/%s?t=zip", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String stateUrl(String ark, int version) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/state/%d/%s/%d?t=json", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name()),
                        version
                );
        }

        public String ingestLinkUrl(String ark, int version) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/ingestlink/%d/%s/%d", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name()),
                        version
                );
        }

        public String versionLinkUrl(String ark, int version) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/versionlink/%d/%s/%d", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name()),
                        version
                );
        }

        public String assembleObjectUrl(String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/assemble-obj/%d/%s", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String stateUrl(String ark, int version, String path) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/state/%d/%s/%d/%s?t=json", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name()),
                        version,
                        URLEncoder.encode(path, StandardCharsets.UTF_8.name())
                );
        }

        public String fixityUrl(String ark, int version, String path) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/fixity/%d/%s/%d/%s?t=json", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name()),
                        version,
                        URLEncoder.encode(path, StandardCharsets.UTF_8.name())
                );
        }

        public String downloadUrl(String ark, int version, String path) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/content/%d/%s/%d/%s?t=json", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name()),
                        version,
                        URLEncoder.encode(path, StandardCharsets.UTF_8.name())
                );
        }

        public String presignFileUrl(String ark, int version, String path) throws UnsupportedEncodingException {
                String key = String.format("%s|%d|%s", ark, version, path);
                return String.format(
                        "http://localhost:%d/%s/presign-file/%d/%s", 
                        port, 
                        cp, 
                        node,
                        URLEncoder.encode(key, StandardCharsets.UTF_8.name())
                );
        }

        public String manifestUrl(String ark) throws UnsupportedEncodingException {
                return String.format(
                        "http://localhost:%d/%s/manifest/%d/%s", 
                        port, 
                        cp,
                        node,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
        }

        public String tokenRetrieveUrl(String token) {
                return String.format(
                        "http://localhost:%d/%s/presign-obj-by-token/%s", 
                        port, 
                        cp,
                        token
                );
        }

        public JSONObject assembleObject(String url) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);

                        //List<NameValuePair> params = new ArrayList<NameValuePair>();
                        //post.setEntity(new UrlEncodedFormEntity(params));

                        HttpResponse response = client.execute(post);
                        assertEquals(200, response.getStatusLine().getStatusCode());
    
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        JSONObject json =  new JSONObject(s);
                        return json;
                }

        }

        //@Test
        public void PreloadedIntoContainerTest() throws IOException, JSONException, ParserConfigurationException, SAXException, XPathExpressionException {
                String ark = "ark:/5555/5555";

                try {    
                        JSONObject json = getJsonContent(stateUrl(ark), 200);
                        verifyObject(json, ark, 1, 1);

                        json = getJsonContent(fixityUrl(ark), 200);
                        verifyObjectFixity(json);

                        json = getJsonContent(stateUrl(ark, 1), 200);
                        verifyVersion(json, ark, 1, 1);
        
                        String path = "producer/hello.txt";
                        json = getJsonContent(stateUrl(ark, 1, path), 200);
                        //hello.txt has 5 bytes
                        verifyFile(json, path, 5);

                        json = getJsonContent(fixityUrl(ark, 1, path), 200);
                        //hello.txt has the checksum listed below
                        verifyFileFixity(json, "sha256=2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");

                        String s = getContent(downloadUrl(ark, 1, path), 200);
                        //hello.txt contains the string "hello"
                        assertEquals("hello", s);
                } catch(Exception e) {
                        e.printStackTrace();
                        throw e;
                } catch(AssertionError e) {
                        e.printStackTrace();
                        throw e;
                } finally {
                }
        }

        @Test
        public void AddObjectTest() throws Exception {
                String ark = "ark:/1111/2222";
                String checkm = "src/test/resources/object1.checkm";

                try {
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        //Version 1 has 7 files
                        verifyVersion(json, ark, 1, 7);
        
                        json = getJsonContent(stateUrl(ark), 200);
                        //Version 1 has 7 files
                        verifyObject(json, ark, 1, 7);

                        json = getJsonContent(fixityUrl(ark), 200);
                        verifyObjectFixity(json);

                        List<String> entries = getZipContent(downloadObjectUrl(ark), 200);
                        //Entry list has 7 files + a manifest
                        assertEquals(8, entries.size());
                        assertTrue(entries.contains("ark+=1111=2222/1/producer/hello.txt"));

                        entries = getZipContent(downloadProducerUrl(ark), 200);
                        //Entry list has 1 producer file
                        assertEquals(1, entries.size());
                        assertTrue(entries.contains("hello.txt"));

                        json = getJsonContent(stateUrl(ark, 1), 200);
                        //Version 1 has 7 files
                        verifyVersion(json, ark, 1, 7);
        
                        String path = "producer/hello.txt";
                        json = getJsonContent(stateUrl(ark, 1, path), 200);
                        //hello.txt has 5 bytes
                        verifyFile(json, path, 5);

                        json = getJsonContent(fixityUrl(ark, 1, path), 200);
                        //hello.txt has the checksum listed below
                        verifyFileFixity(json, "sha256=2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");

                        String s = getContent(downloadUrl(ark, 1, path), 200);
                        //hello.txt contains the string "hello"
                        assertEquals("hello", s);

                        json = getJsonContent(presignFileUrl(ark, 1, path), 200);
                        verifyPresignFile(json);

                        s = getContent(manifestUrl(ark), 200);
                        Document d = getDocument(s, "objectInfo");
                        //object manifest has 7 files
                        verifyObjectInfo(d, ark, 1, 7);        

                        s = getContent(ingestLinkUrl(ark, 1), 200);
                        //ingest link has 2 files + a manifest
                        verifyIngestLink(s, 3);
                        assertFalse(s.isEmpty());
                } catch(Exception e) {
                        e.printStackTrace();
                        throw e;
                } catch(AssertionError e) {
                        e.printStackTrace();
                        throw e;
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);        
                }
        }

        @Test
        public void CopyObjectTest() throws UnsupportedEncodingException, IOException, JSONException {
                String ark = "ark:/1111/6666";
                String checkm = "src/test/resources/object1.checkm";

                try {
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        //Version 1 has 7 files
                        verifyVersion(json, ark, 1, 7);

                        json = getJsonContent(stateUrl(ark), 200);
                        //Version 1 has 7 files
                        verifyObject(json, ark, 1, 7);

                        json = copyObject(copyUrl(ark, 8888));
                        //Version 1 has 7 files
                        verifyObject(json, ark, 1, 7);

                        json = getJsonContent(nodeStateUrl(8888, ark), 200);
                        //Version 1 has 7 files
                        verifyObject(json, ark, 1, 7);
                } catch(Exception e) {
                        e.printStackTrace();
                        throw e;
                } catch(AssertionError e) {
                        e.printStackTrace();
                        throw e;
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);        

                        deleteObject(deleteUrl(8888, ark));
                        getContent(stateUrl(ark), 404);        
                }
        }

        @Test
        public void AddObjectAndVerTest() throws Exception {
                String ark = "ark:/1111/3333";
                String checkm = "src/test/resources/object1.checkm";
                String checkmv2 = "src/test/resources/object1v2.checkm";

                try {
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        //version 1 has 7 files
                        verifyVersion(json, ark, 1, 7);
        
                        json = addObjectByManifest(updateUrl(ark), checkmv2);
                        //version 2 has 8 files (7+1)
                        verifyVersion(json, ark, 2, 8);

                        json = getJsonContent(stateUrl(ark), 200);
                        //object has 8 files
                        verifyObject(json, ark, 2, 8);
        
                        List<String> entries = getZipContent(downloadObjectUrl(ark), 200);
                        //object zip file has 9 entries: 7 (v1) + 1 (v2) + 1 (manifest)
                        assertEquals(9, entries.size());
                        assertTrue(entries.contains("ark+=1111=3333/1/producer/hello.txt"));
                        assertTrue(entries.contains("ark+=1111=3333/2/producer/hello2.txt"));

                        entries = getZipContent(downloadProducerUrl(ark), 200);
                        //producer zip file has 2 entries
                        assertEquals(2, entries.size());
                        assertTrue(entries.contains("hello.txt"));
                        assertTrue(entries.contains("hello2.txt"));

                        json = getJsonContent(stateUrl(ark, 2), 200);
                        //version 2 has 8 files
                        verifyVersion(json, ark, 2, 8);
        
                        String path = "producer/hello.txt";
                        json = getJsonContent(stateUrl(ark, 1, path), 200);
                        //file hello.txt is 5 bytes
                        verifyFile(json, path, 5);

                        path = "producer/hello2.txt";
                        json = getJsonContent(stateUrl(ark, 2, path), 200);
                        //file hello.txt is 5 bytes
                        verifyFile(json, path, 5);

                        String s = getContent(manifestUrl(ark), 200);
                        Document d = getDocument(s, "objectInfo");
                        //object manifest has 8 files
                        verifyObjectInfo(d, ark, 2, 8);
                        
                        s = getContent(ingestLinkUrl(ark, 2), 200);
                        //ingest link has 3 files + a manifest
                        verifyIngestLink(s, 4);
                        assertFalse(s.isEmpty());

                        json = getJsonContent(versionLinkUrl(ark, 2), 200);
                        //version link has 8 files
                        verifyVersionLink(json, ark, 8);
                } catch(Exception e) {
                        e.printStackTrace();
                        throw e;
                } catch(AssertionError e) {
                        e.printStackTrace();
                        throw e;
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);        
                }
        }

        @Test
        public void AddObjectAndVerNoChangeTest() throws Exception {
                String ark = "ark:/1111/5555";
                String checkm = "src/test/resources/object1.checkm";
                String checkmv2 = "src/test/resources/object1v2.checkm";
                String checkmv3 = "src/test/resources/object1v3.checkm";

                try {
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        //version 1 has 7 files
                        verifyVersion(json, ark, 1, 7);
        
                        json = addObjectByManifest(updateUrl(ark), checkmv2);
                        //version 2 has 8 files (7+1)
                        verifyVersion(json, ark, 2, 8);

                        json = addObjectByManifest(updateUrl(ark), checkmv3);
                        //version 2 has 8 files (7+1)
                        verifyVersion(json, ark, 3, 8);

                        json = getJsonContent(stateUrl(ark), 200);
                        //object has 8 files
                        verifyObject(json, ark, 3, 8);
        
                        List<String> entries = getZipContent(downloadObjectUrl(ark), 200);
                        //object zip file has 9 entries: 7 (v1) + 1 (v2) + 0 (v3) + 1 (manifest)
                        assertEquals(9, entries.size());
                        assertTrue(entries.contains("ark+=1111=5555/1/producer/hello.txt"));
                        assertTrue(entries.contains("ark+=1111=5555/2/producer/hello2.txt"));
                        assertFalse(entries.contains("ark+=1111=5555/3/producer/hello.txt"));

                        entries = getZipContent(downloadProducerUrl(ark), 200);
                        //producer zip file has 2 entries
                        assertEquals(2, entries.size());
                        assertTrue(entries.contains("hello.txt"));
                        assertTrue(entries.contains("hello2.txt"));

                        json = getJsonContent(stateUrl(ark, 3), 200);
                        //version 2 has 8 files
                        verifyVersion(json, ark, 3, 8);
        
                        String s = getContent(manifestUrl(ark), 200);
                        Document d = getDocument(s, "objectInfo");
                        //object manifest has 8 files
                        verifyObjectInfo(d, ark, 3, 8);
                        
                        s = getContent(ingestLinkUrl(ark, 3), 200);
                        //ingest link has 3 files + a manifest
                        verifyIngestLink(s, 4);
                        assertFalse(s.isEmpty());

                        json = getJsonContent(versionLinkUrl(ark, 3), 200);
                        //version link has 8 files
                        verifyVersionLink(json, ark, 8);
                } catch(Exception e) {
                        e.printStackTrace();
                        throw e;
                } catch(AssertionError e) {
                        e.printStackTrace();
                        throw e;
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);        
                }
        }

        @Test
        public void AddObjectAndAssemble() throws Exception {
                String ark = "ark:/1111/4444";
                String checkm = "src/test/resources/object1.checkm";
                String checkmv2 = "src/test/resources/object1v2.checkm";

                try {
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        //version 1 has 7 files
                        verifyVersion(json, ark, 1, 7);
        
                        json = addObjectByManifest(updateUrl(ark), checkmv2);
                        //version 2 has 8 files (7+1)
                        verifyVersion(json, ark, 2, 8);

                        json = assembleObject(assembleObjectUrl(ark));
                        assertEquals(200, json.getInt("status"));
                        assertEquals("Request queued, use token to check status", json.get("message"));                        

                        String token = json.getString("token");
                        int attempt = 0;
                        json = getJsonContent(tokenRetrieveUrl(token), 0);
                        while(json.getInt("status") == 202 && attempt < ASSM_TRIES) {
                                attempt++;
                                Thread.sleep(ASSM_WAIT);
                                json = getJsonContent(tokenRetrieveUrl(token), 0);
                        }

                        assertEquals(200, json.getInt("status"));
                        assertTrue(json.has("url"));
                        assertEquals("Payload contains token info", json.get("message"));                        
                } catch(Exception e) {
                        e.printStackTrace();
                        throw e;
                } catch(AssertionError e) {
                        e.printStackTrace();
                        throw e;
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);        
                }
        }

        public void manageLock(String url, boolean val) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);

                        //System.out.println(url);

                        HttpResponse response = client.execute(post);
                        assertEquals(200, response.getStatusLine().getStatusCode());
    
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        JSONObject json =  new JSONObject(s);

                        //System.out.println(json.toString(2));

                        JSONObject v = json.getJSONObject("tok:zooTokenState");
                        assertEquals(val, v.getBoolean("tok:tokenStatus"));
                }
        }

        @Test
        public void AddObjectAndAssembleWithQueueLock() throws Exception {
                String ark = "ark:/1111/5555";
                String checkm = "src/test/resources/object1.checkm";
                String checkmv2 = "src/test/resources/object1v2.checkm";

                manageLock(lockUrl(), true);
                try  {                        
                        JSONObject json = addObjectByManifest(addUrl(ark), checkm);
                        //version 1 has 7 files
                        verifyVersion(json, ark, 1, 7);
        
                        json = addObjectByManifest(updateUrl(ark), checkmv2);
                        //version 2 has 8 files (7+1)
                        verifyVersion(json, ark, 2, 8);

                        json = assembleObject(assembleObjectUrl(ark));
                        assertEquals(200, json.getInt("status"));
                        assertEquals("Request queued, use token to check status", json.get("message"));                        

                        String token = json.getString("token");
                        int attempt = 0;
                        json = getJsonContent(tokenRetrieveUrl(token), 0);
                        while(json.getInt("status") == 202 && attempt < ASSM_TRIES) {
                                attempt++;
                                Thread.sleep(ASSM_WAIT);
                                json = getJsonContent(tokenRetrieveUrl(token), 0);
                        }

                        assertEquals(202, json.getInt("status"));

                        manageLock(unlockUrl(), false);

                        json = getJsonContent(tokenRetrieveUrl(token), 0);
                        attempt = 0;
                        while(json.getInt("status") == 202 && attempt < ASSM_TRIES) {
                                attempt++;
                                Thread.sleep(ASSM_WAIT);
                                json = getJsonContent(tokenRetrieveUrl(token), 0);
                        }
                        assertEquals(200, json.getInt("status"));

                        assertTrue(json.has("url"));
                        assertEquals("Payload contains token info", json.get("message"));                        
                } catch(Exception e) {
                        e.printStackTrace();
                        throw e;
                } catch(AssertionError e) {
                        e.printStackTrace();
                        throw e;
                } finally {
                        deleteObject(deleteUrl(ark));
                        getContent(stateUrl(ark), 404);
                        manageLock(unlockUrl(), false);        
                }
        }

}