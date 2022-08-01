/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store.test;

import java.io.InputStream;
import java.net.URL;
import org.apache.http.client.utils.URIBuilder;
import org.cdlib.mrt.utility.HttpGet;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;

import org.json.JSONObject;

/**
 *
 * @author replic
 */
public class TestHostname 
{

    protected static final String NAME = "TestHostname";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    
    
    public static void main(String[] args) 
            throws Exception
    {
        LoggerInf logger = new TFileLogger(NAME, 10, 10);
        String getHostnameurlS = "http://localhost:35121/storage/hostname";
        String stateUrlS = "http://store-stg.cdlib.org:35121/storage/state?t=xml";
        String canonicalHostname = getCanonicalHostname(getHostnameurlS, logger);
        System.out.println("canonicalHostname:" + canonicalHostname);
        System.out.println("Input:" + stateUrlS);
        URIBuilder builder = new URIBuilder(stateUrlS);
        builder.setHost(canonicalHostname);
        URL stateUrl = builder.build().toURL();
        System.out.println("StateUrl:" + stateUrl.toString());
        InputStream stateResponseIO = HttpGet.getStream(stateUrl, 0, logger);
        String stateResponse = StringUtil.streamToString(stateResponseIO, "utf-8");
        System.out.println("stateResponseIO: \n"
                + " - stateResponse:\n" + stateResponse + "\n"
        );
    }    
    
    public static String getCanonicalHostname(String getUrlS, LoggerInf logger) 
            throws Exception
    {
        URL getUrl = new URL(getUrlS);
        InputStream hostnameIO = HttpGet.getStream(getUrl, 0, logger);
        String hostResponse = StringUtil.streamToString(hostnameIO, "utf-8");
        System.out.println("Hostinfo: \n"
                + " - hostResponse:\n" + hostResponse + "\n"
        );
        JSONObject json = new JSONObject(hostResponse);
        String hostname = json.getString("hostname");
        String canonicalHostname = json.getString("canonicalHostname");
        String hostAddress = json.getString("hostAddress");
        System.out.println("Hostinfo: \n"
                + " - hostname:" + hostname + "\n"
                + " - canonicalHostname:" + canonicalHostname+ "\n"
                + " - hostAddress:" + hostAddress+ "\n"
        );
        return canonicalHostname;
    }
}