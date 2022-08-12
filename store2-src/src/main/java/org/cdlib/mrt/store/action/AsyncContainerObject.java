/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/
package org.cdlib.mrt.store.action;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.core.StreamingOutput;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;


import org.cdlib.mrt.store.email.StorageEmail;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TallyTable;
/**
 * Async container for creating a container object
 * @author dloy
 */
public class AsyncContainerObject
        extends ActionAbs
        implements Runnable
{

    private static final String NAME = "AsyncContainerObject";
    private static final String MESSAGE = NAME + ": ";
    
    protected static final boolean DEBUG = false;
    protected static final boolean STATS = false;
    protected static final String WRITE = "write";
    protected static final String NL = System.getProperty("line.separator");
    private final String ERRDELIM = NL + "___________________________________" + NL;
    private static final boolean THROWEXCEPTION = false;
    protected StreamingOutput streamOutput = null;
    protected File outputFile = null;
    protected String email = null;
    protected Properties storageProp = null;
    protected TallyTable stats = new TallyTable();
    protected StorageEmail storageEmail = null;
    protected ArrayList<String> recipients = new ArrayList<String>();
    protected ArrayList<String> recipientsBCC = new ArrayList<String>();
    protected String[] recipientsA= null;
    protected String from = null;
    protected String subject = null;
    protected String msg = null;
    protected String body = null;
    protected String formatType = null;
    protected String name = null;
    protected ArrayList<String> errBCC = new ArrayList<>();
    protected String errMsg = null;
    protected CloudStoreInf cloudStore = null;
    protected String bucket = null;
    protected String outKey = null;
    
    public static AsyncContainerObject getAsyncContainerObject(
            Properties storageProp,
            StreamingOutput streamOutput,
            File outputFile,
            String name,
            String email,
            LoggerInf logger)
        throws TException
    {
        return new AsyncContainerObject(storageProp, streamOutput, outputFile, name, email, logger);
    }
    
    protected AsyncContainerObject(
            Properties storageProp,
            StreamingOutput streamOutput,
            File outputFile,
            String name,
            String email,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.storageProp = storageProp;
        this.streamOutput = streamOutput;
        this.outputFile = outputFile;
        this.name = name;
        this.email = email;
        validate();
    }
    private void validate()
        throws TException
    {
        if (storageProp == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "storageProp null");
        }
        if (storageProp.size() == 0) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "empty storageProp");
        }
        if (streamOutput == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "streamOutput null");
        }
        if (outputFile == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "outputFile null");
        }
        if (email == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "email null");
        }
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "logger null");
        }
        setEmail();
    }
    
    private void setEmail()
        throws TException
    {
        try {
            storageEmail = new StorageEmail(storageProp);
            for (int i=1; true; i++) {
                String recipientBCC = storageProp.getProperty("BCC." + i);
                if (recipientBCC == null) break;
                errBCC.add(recipientBCC);
                if (DEBUG) System.out.println("AddBCC:" + recipientBCC); //!
            }
            errMsg = storageProp.getProperty("errMsg");
            if (StringUtil.isNotEmpty(errMsg)) {
                errMsg = errMsg.replace("\n", NL);
            } else {
                errMsg = "Error occurred during processing";
            }
            if (DEBUG) System.out.println("errMsg:" + errMsg);
            extractEmail(email);

        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            streamToFile(streamOutput, outputFile);
            if (!outputFile.exists() || (outputFile.length() <= 0)) {
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "Async file does not exist:"
                        + outputFile.getCanonicalPath());
            }
            if (DEBUG) System.out.println("File build:" + outputFile.getCanonicalPath());
            copyCloud(storageProp, name, outputFile, logger);
            sendEmail(body, msg);

        } catch (Exception ex) {
            String msgx = MESSAGE + "Exception for "
                    + " - Exception:" + ex
                    ;
            System.out.println("Exception: " + msgx);
            logger.logError(msgx, 2);
            sendErrEmail(body, msg);
            setException(ex);

        }

    }

    public void callEx()
        throws TException
    {
        run();
        throwEx();
    }
    
    protected static void streamToFile(
            StreamingOutput outStream,
            File outFile)
        throws TException
    {
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(outFile);
            if(THROWEXCEPTION) throw new TException.GENERAL_EXCEPTION("TEST");
            outStream.write(stream);
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
            try {
                stream.close();
            } catch (Exception ex) { }
        }
        
    }
    
    protected static void copyCloud(
            Properties storageProp,
            String outKey,
            File inputFile,
            LoggerInf logger)
        throws TException
    {
        
        OutputStream stream = null;
        try {
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("copyCloud-storageProp", storageProp));
            String storageServiceS = storageProp.getProperty("StorageService");
            if (storageServiceS == null) return;
            Properties prop = PropertiesUtil.loadPropertiesFileName(storageServiceS + "/store-info.txt");
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("copyCloud-prop", prop));
            String archiveNodeName = prop.getProperty("nodePath");
            String archiveNode = prop.getProperty("archiveNode");
            if ((archiveNodeName == null) || (archiveNode == null)) {
                return;
            }
            long outNode = 0;
            try {
                outNode = Long.parseLong(archiveNode);
            } catch (Exception ex) {
                return;
            }
            NodeIO nodeIO = NodeIO.getNodeIOConfig(archiveNodeName, logger);
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(outNode);
            if (accessNode == null) {
                System.out.println("WARNING cloud node not supported for archive");
                return;
            }
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            System.out.println("copyCloud-action"
                + " - bucket:" + bucket
                + " - outKey:" + outKey
                    );
            CloudResponse response = service.putObject(bucket,outKey, inputFile);
            Exception exc = response.getException();
            if (exc != null) {
                throw exc;
            }
            try {
                String deleteName = inputFile.getCanonicalPath();
                inputFile.delete();
                if (DEBUG) System.out.println("Archive deleted:" + deleteName);
            } catch (Exception deleteEx) {
                System.out.println("WARNING: Archive deletion exception:" + deleteEx);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    
    protected void bumpTime(String key, long startTime)
    {
        if (!STATS) return;
        long  difftime = getTime() - startTime;
        stats.bump(key, difftime);
    }
    
    protected long getTime()
    {
        return System.nanoTime();
    }
    
    

    /**
    * Send a single email.
    */
    protected void extractEmail(
            String emailProfile)
        throws TException
    {
        /*
         * 
<email> 
    <from>Required - rafe@rafe.us</from> 
    <to>Required - someone@example.com</to> 
    <subject>Required - This is the subject</subject> 
    <msg>Optional - text body of an email message</msg> 
    <body>Required - This is a non-text body of an email message (i.e. html)</body> 
</email>
         */
        try {
            if (StringUtil.isEmpty(emailProfile)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "empty emailProfile");
            }
            ByteArrayInputStream bais = new ByteArrayInputStream(emailProfile.getBytes("utf-8"));
            Document emailDoc = getDocument(bais);
            Element root = emailDoc.getRootElement();
            List<Element> list = root.getChildren();
            for (Iterator iter = list.iterator(); iter.hasNext();) {
                Element elem = (Element) iter.next();
                String eleName = elem.getName();
                eleName = eleName.toLowerCase();
                if (elem.getName().equals("from")) from = elem.getText();
                else if (elem.getName().equals("subject")) subject = elem.getText();
                else if (elem.getName().equals("msg")) {
                    msg = elem.getText();
                    formatType = "txt";
                }
                else if (elem.getName().equals("body")) {
                    body = elem.getText();
                    formatType = "html";
                }
                else if (elem.getName().equals("to")) recipients.add(elem.getText());
                else if (elem.getName().equals("bcc")) recipientsBCC.add(elem.getText());
            }
            recipientsA = recipients.toArray(new String[0]);
            if (StringUtil.isEmpty(from)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing required email property from");
            }
            if (StringUtil.isEmpty(subject)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing required email property subject");
            }
            if (StringUtil.isEmpty(msg) && StringUtil.isEmpty(body)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "email requires either body or msg");
            }
            // used to insert the hostname into the msg or body part
            if (!StringUtil.isEmpty(msg)) {
                msg = insertHost(msg, name);
            } else if (!StringUtil.isEmpty(body)) {
                body = insertHost(body, name);
            }

        } catch (Exception ex){
            System.out.println("sendEmail Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static String insertHost(String message, String name)
        throws TException
    {
        
        if (name == null) return message;
        String hostname = getHostName();
        int pos = message.indexOf(name);
        if (pos < 0) return message;
        String first = message.substring(0,pos);
        String rest = message.substring(pos);
        return first + hostname + "/" + rest;
    }
    
    public static String getHostName()
        throws TException
    {
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            String hostname = addr.getHostName();
            System.out.println("HOSTNAME1: "  + hostname);
            int pos = hostname.indexOf('-');
            int posLast = hostname.lastIndexOf('-');
            if (pos >= 0) {
                hostname = hostname.substring(pos+1);
            }
            
            pos = hostname.indexOf('-');
            if (pos >= 0) {
                hostname = hostname.substring(0, pos);
            }
            System.out.println("HOSTNAME2: "  + hostname);
            return hostname;
        } catch (Exception ex) {
            return null;
        }
    }
    
    /**
     * Input the formatTypes.xml and build a Document
     * @throws TException
     */
    protected static Document getDocument(InputStream inStream)
        throws TException
    {
        try {
            SAXBuilder builder = new SAXBuilder();
            if (DEBUG) System.out.println("Doc built");
            return builder.build(inStream);

        } catch (Exception ex) {
            System.out.println("ObjectFormatXML: Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);

        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (Exception e) { }
            }
        }
    }
    
    protected void sendEmail(String localBody, String localMsg)
    {
        try {
            if (recipientsBCC.size() > 0) {
            	storageEmail.setRecipientsBCC(recipientsBCC);
            }
            if (StringUtil.isEmpty(localBody)) {
                storageEmail.sendEmail(recipientsA, from, subject, localMsg);
            } else {
                storageEmail.sendEmail("message", formatType, recipientsA, from, subject, localMsg, localBody);
            }
        } catch (Exception ex) { return; }
    }
    
    protected void sendErrEmail(String localBody, String localMsg)
    {
        try {
            if (StringUtil.isNotEmpty(localMsg)) {
                localMsg = errMsg + ERRDELIM + localMsg;
            } else {
                localMsg = errMsg;
            }
            subject = "Exception: " + subject;
            //Add default BCC list to the error BCC list
            errBCC.addAll(recipientsBCC);
            storageEmail.setRecipientsBCC(errBCC);
            if (StringUtil.isEmpty(localMsg)) {
                storageEmail.sendEmail(recipientsA, from, subject, localMsg);
            } else {
                storageEmail.sendEmail("message", formatType, recipientsA, from, subject, localMsg, localBody);
            }
        } catch (Exception ex) { return; }
    }
}

