package org.cdlib.mrt.store;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

import javax.servlet.GenericServlet;
import javax.servlet.ServletConfig;
import javax.ws.rs.core.Response;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.cdlib.mrt.store.app.StorageServiceInit;
import org.cdlib.mrt.store.app.ValidateCmdParms;
import org.cdlib.mrt.store.app.jersey.JerseyBase.FormatType;
import org.cdlib.mrt.store.storage.StorageServiceInf;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;

public class StorageIT {
        private int port = 9001;

        public StorageIT() {
                try {
                        port = Integer.parseInt(System.getenv("minio-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("minio-server.port not set");
                }
        }

        @Test
        public void connectToMinioDocker() throws HttpResponseException, IOException {
                String url = String.format("http://localhost:%d", port);
                /*
                When using java11 libraries, the following classes can be used...
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .build();
                HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
                */
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    String responseString = new BasicResponseHandler().handleResponse(response);
                    assertFalse(responseString.isEmpty());
              }
                
        }

        //@Test
        public void SimpleTest() throws TException {
                ServletConfig sc = null;
                StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
                StorageServiceInf storageService = storageServiceInit.getStorageService();
                LoggerInf nodeLogger = storageService.getLogger(7777);
                StateInf responseState = storageService.getNodeState(7777);
        }

}
