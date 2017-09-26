
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.store.je;

import java.net.URL;
import org.cdlib.mrt.utility.URLEncoder;
import java.util.List;
import java.util.Properties;

import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;

import org.apache.http.Header;



import java.io.File;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.NameValuePair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;


/**
 * AddClient for Storage
 * @author  loy
 */

public class StorageIDClient
{
    private static final String NAME = "StorageAddClient";
    private static final String MESSAGE = NAME + ": ";
    protected final static String NL = System.getProperty("line.separator");
    protected final static String FORMAT_NAME = "t";
    protected final boolean DEBUG = true;

    protected LoggerInf logger = null;

    public StorageIDClient(LoggerInf logger)
    {
        this.logger = logger;
    }

    public StorageIDClient()
    {
        this.logger = new TFileLogger(NAME, 5, 5);
    }

    public Properties getPrimaryID(
            String link,
            String nodeIDS,
            String localContext,
            String localID,
            String formatType,
            int timeout)
        throws TException
    {
        File testFile = null;
        try {
            if (StringUtil.isEmpty(link)) {
                throw new TException.INVALID_OR_MISSING_PARM("link required");
            }
            if (StringUtil.isEmpty(localContext)) {
                throw new TException.INVALID_OR_MISSING_PARM("localContext required");
            }
            if (StringUtil.isEmpty(localID)) {
                throw new TException.INVALID_OR_MISSING_PARM("localID required");
            }
            Integer nodeID = null;
            if (StringUtil.isNotEmpty(nodeIDS)) {
                try {
                    nodeID = Integer.parseInt(nodeIDS);
                } catch (Exception ex) { }
                if (nodeID == 0) {
                    throw new TException.INVALID_OR_MISSING_PARM("nodeID invalid");
                }
            }

            HttpResponse  response = sendGetPrimaryID(
                link,
                nodeID,
                localContext,
                localID,
                formatType,
                timeout);
            return processResponse(response);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 3);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            if (testFile != null) {
                try {
                    testFile.delete();
                } catch (Exception ex) { }
            }
        }

    }


    public Properties getLocalIDs(
            String link,
            String nodeIDS,
            String primaryID,
            String formatType,
            int timeout)
        throws TException
    {
        File testFile = null;
        try {
            if (StringUtil.isEmpty(link)) {
                throw new TException.INVALID_OR_MISSING_PARM("link required");
            }
            if (StringUtil.isEmpty(primaryID)) {
                throw new TException.INVALID_OR_MISSING_PARM("primaryID required");
            }
            Integer nodeID = null;
            if (StringUtil.isNotEmpty(nodeIDS)) {
                try {
                    nodeID = Integer.parseInt(nodeIDS);
                } catch (Exception ex) { }
                if (nodeID == 0) {
                    throw new TException.INVALID_OR_MISSING_PARM("nodeID invalid");
                }
            }

            HttpResponse  response = sendGetLocalIDs(
                link,
                nodeID,
                primaryID,
                formatType,
                timeout);
            return processResponse(response);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 3);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            if (testFile != null) {
                try {
                    testFile.delete();
                } catch (Exception ex) { }
            }
        }

    }

    public Properties deleteLocalID(
            String link,
            String nodeIDS,
            String localContext,
            String localID,
            String formatType,
            int timeout)
        throws TException
    {
        File testFile = null;
        try {
            if (StringUtil.isEmpty(link)) {
                throw new TException.INVALID_OR_MISSING_PARM("link required");
            }
            if (StringUtil.isEmpty(localContext)) {
                throw new TException.INVALID_OR_MISSING_PARM("localContext required");
            }
            if (StringUtil.isEmpty(localID)) {
                throw new TException.INVALID_OR_MISSING_PARM("localID required");
            }
            Integer nodeID = null;
            if (StringUtil.isNotEmpty(nodeIDS)) {
                try {
                    nodeID = Integer.parseInt(nodeIDS);
                } catch (Exception ex) { }
                if (nodeID == 0) {
                    throw new TException.INVALID_OR_MISSING_PARM("nodeID invalid");
                }
            }

            HttpResponse  response = sendDeleteID(
                link,
                nodeID,
                localContext,
                localID,
                null,
                formatType,
                timeout);
            return processResponse(response);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 3);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            if (testFile != null) {
                try {
                    testFile.delete();
                } catch (Exception ex) { }
            }
        }

    }

    public Properties deletePrimaryID(
            String link,
            String nodeIDS,
            String primaryID,
            String formatType,
            int timeout)
        throws TException
    {
        File testFile = null;
        File manifestFile = null;
        try {
            if (StringUtil.isEmpty(link)) {
                throw new TException.INVALID_OR_MISSING_PARM("link required");
            }
            if (StringUtil.isEmpty(primaryID)) {
                throw new TException.INVALID_OR_MISSING_PARM("primaryID required");
            }
            Integer nodeID = null;
            if (StringUtil.isNotEmpty(nodeIDS)) {
                try {
                    nodeID = Integer.parseInt(nodeIDS);
                } catch (Exception ex) { }
                if (nodeID == 0) {
                    throw new TException.INVALID_OR_MISSING_PARM("nodeID invalid");
                }
            }

            HttpResponse  response = sendDeleteID(
                link,
                nodeID,
                null,
                null,
                primaryID,
                formatType,
                timeout);
            return processResponse(response);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 3);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            if (testFile != null) {
                try {
                    testFile.delete();
                } catch (Exception ex) { }
            }
        }

    }
    public HttpResponse sendDeleteID(
            String link,
            Integer nodeID,
            String localContext,
            String localID,
            String primaryID,
            String formatType,
            int timeout)
        throws TException
    {
        try {
            String nodeIDS = "";
            String type = null;
            String id = null;
            if ((localContext == null) && (localID == null) && (primaryID == null)) {
                throw new TException.INVALID_OR_MISSING_PARM("Error: identifier not provided");
            }
            if ((localContext != null) && (localID != null) && (primaryID != null)) {
                throw new TException.INVALID_OR_MISSING_PARM("Error: both identifier types provided");
            }

            if (nodeID != null) {
                nodeIDS = "/" + nodeID;
            }
            if ((localContext != null) && (localID != null)) {
                type = "localid";
                id = "/" + URLEncoder.encode(localContext, "utf-8")
                        + "/" + URLEncoder.encode(localID, "utf-8");

            } else {
                type = "primaryid";
                id = "/" + URLEncoder.encode(primaryID, "utf-8");
            }
            String formatTypeDisp = "?" + FORMAT_NAME + "=xhtml";
            if (StringUtil.isNotEmpty(formatType)) formatTypeDisp = "?" + FORMAT_NAME + "=" + formatType;
            String deleteURLS = link
                    + "/" + type
                    + nodeIDS
                    + id
                    + formatTypeDisp;

            URL deleteURL = new URL(deleteURLS);
            log(MESSAGE + "sendDeleteID:"
                    + " - deleteURL=" + deleteURL
                    + " - link=" + link
                    + " - nodeID=" + nodeID
                    + " - localContext=" + localContext
                    + " - localID=" + localID
                    + " - primaryID=" + primaryID
                    + " - formatType=" + formatType
                    ,10);

            HttpClient httpClient = HTTPUtil.getHttpClient(timeout);
            HttpDelete httpDelete = new HttpDelete(deleteURL.toString());
            System.out.println("executing request " + httpDelete.getRequestLine());
            HttpResponse response = httpClient.execute(httpDelete);
            return response;

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 3);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(ex);

        }
    }

    public HttpResponse sendGetPrimaryID(
            String link,
            Integer nodeID,
            String localContext,
            String localID,
            String formatType,
            int timeout)
        throws TException
    {
        try {
            //"primary/{nodeid}/{context}/{localidid}"
            String nodeIDS = "";
            String type = null;
            String id = null;
            if ((localContext == null) || (localID == null)) {
                throw new TException.INVALID_OR_MISSING_PARM("Error: local context and id required");
            }

            if (nodeID != null) {
                nodeIDS = "/" + nodeID;
            }
                type = "primary";
                id = "/" + URLEncoder.encode(localContext, "utf-8")
                        + "/" + URLEncoder.encode(localID, "utf-8");
            String formatTypeDisp = "?" + FORMAT_NAME + "=xhtml";
            if (StringUtil.isNotEmpty(formatType)) formatTypeDisp = "?" + FORMAT_NAME + "=" + formatType;
            String getURLS = link
                    + "/" + type
                    + nodeIDS
                    + id
                    + formatTypeDisp;

            URL getURL = new URL(getURLS);
            System.out.println("sendGetPrimaryID URL:" + getURL.toString());
            
            log(MESSAGE + "sendDeleteID:"
                    + " - deleteURL=" + getURL
                    + " - link=" + link
                    + " - nodeID=" + nodeID
                    + " - localContext=" + localContext
                    + " - localID=" + localID
                    + " - formatType=" + formatType
                    ,10);
            HttpClient httpClient = HTTPUtil.getHttpClient(timeout);
            HttpGet httpGet = new HttpGet(getURL.toString());
            System.out.println("executing request " + httpGet.getRequestLine());
            HttpResponse response = httpClient.execute(httpGet);
            return response;

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 3);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(ex);

        }
    }

    public HttpResponse sendGetLocalIDs(
            String link,
            Integer nodeID,
            String primaryID,
            String formatType,
            int timeout)
        throws TException
    {
        try {
            //"local/{nodeid}/{primaryid}"
            String nodeIDS = "";
            String type = null;
            String id = null;
            if (primaryID == null) {
                throw new TException.INVALID_OR_MISSING_PARM("Error: primary id required");
            }

            if (nodeID != null) {
                nodeIDS = "/" + nodeID;
            }
                type = "local";
                id = "/" + URLEncoder.encode(primaryID, "utf-8");

            String formatTypeDisp = "?" + FORMAT_NAME + "=xhtml";
            if (StringUtil.isNotEmpty(formatType)) formatTypeDisp = "?" + FORMAT_NAME + "=" + formatType;
            String getURLS = link
                    + "/" + type
                    + nodeIDS
                    + id
                    + formatTypeDisp;

            URL getURL = new URL(getURLS);
            System.out.println("sendGetPrimaryID URL:" + getURL.toString());

            log(MESSAGE + "sendGetLocalIDs:"
                    + " - url=" + getURL
                    + " - link=" + link
                    + " - nodeID=" + nodeID
                    + " - primaryID=" + primaryID
                    + " - formatType=" + formatType
                    ,10);
            HttpClient httpClient = HTTPUtil.getHttpClient(timeout);
            HttpGet httpGet = new HttpGet(getURL.toString());
            System.out.println("executing request " + httpGet.getRequestLine());
            HttpResponse response = httpClient.execute(httpGet);
            return response;

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 3);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(ex);

        }
    }

    public void log(String msg, int lvl)
    {
        System.out.println(msg);
    }

    protected void addNameValue(List <NameValuePair> nvps, String key, String value)
    {
        if (StringUtil.isEmpty(key)) return;
        if (StringUtil.isEmpty(value)) return;
        nvps.add(new BasicNameValuePair(key, value));
    }


    protected Properties processResponse(HttpResponse response)
        throws TException
    {
        try {
            Properties resultProp = new Properties();
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();


            resultProp.setProperty("response.status", "" + statusCode);
            if ((statusCode >= 300) || (statusCode < 200)) {
                resultProp.setProperty("error.status", "" + statusCode);
            }
            HttpEntity resEntity = response.getEntity();
            String responseState = StringUtil.streamToString(resEntity.getContent(), "utf-8");
            if (StringUtil.isNotEmpty(responseState)) {
                resultProp.setProperty("response.state", responseState);
                if (DEBUG) System.out.println("mrt-response:" + responseState);
            }
            Header [] headers = response.getAllHeaders();
            for (Header header : headers) {
                resultProp.setProperty(
                        "header." + header.getName(),
                        header.getValue());
            }
            if (DEBUG) {
                System.out.println(PropertiesUtil.dumpProperties("!!!!sendArchiveMultipart!!!!", resultProp, 100));

                System.out.println("----------------------------------------");
                System.out.println(response.getStatusLine());
                if (resEntity != null) {
                    System.out.println("Response content length: " + resEntity.getContentLength());
                    System.out.println("Chunked?: " + resEntity.isChunked());
                }
            }
            if (resEntity != null) {
                EntityUtils.consume(resEntity);
                resEntity.consumeContent();
            }
            return resultProp;

        } catch (Exception ex) {
            String msg = "Exception:" + StringUtil.stackTrace(ex);
            log(MESSAGE + "Exception:" + StringUtil.stackTrace(ex), 0);
            System.out.println(msg);
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }

    }

}
