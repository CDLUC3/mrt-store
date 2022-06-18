package org.cdlib.mrt.store;

import org.junit.Test;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Context;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.File;
import java.io.Writer;
import java.io.IOException;
import static org.junit.Assert.*;

public class EmbedTestIT {
    private Tomcat tomcat;
    private int port = 8080;
    private String cp = "/embedded-jar";

    public EmbedTestIT() throws LifecycleException, ServletException, InterruptedException {
        tomcat = new Tomcat();
        tomcat.setPort(port);

        tomcat.addWebapp(cp, new File("target/mrt-storewar-it-1.0-SNAPSHOT").getAbsolutePath());

        tomcat.start();

        System.out.println("Tomcat started");
        Thread.sleep(100000);

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
    public void SimpleTest() {
        String url = String.format("http://localhost:%d/%s/state", port, cp);
        String s = getContent(url);
        System.out.println(s);
    }

}
