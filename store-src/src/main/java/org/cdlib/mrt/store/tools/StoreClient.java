/*
Copyright (c) 2005-2010, Regents of the University of California
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
*********************************************************************/
package org.cdlib.mrt.store.tools;

import org.cdlib.mrt.store.can.*;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import org.cdlib.mrt.utility.URLEncoder;
import java.util.Properties;



import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.app.jersey.KeyNameHttpInf;
import org.cdlib.mrt.store.app.ValidateCmdParms;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;


/**
 *
 * @author dloy
 */
public class StoreClient

{
    protected static final String NAME = "StoreClient";
    protected static final String MESSAGE = NAME + ": ";

    protected boolean debug = false;
    protected LoggerInf logger = null;
    protected URL storeURL = null;



    public static StoreClient getStoreClient(String storeURL, LoggerInf logger)
        throws TException
    {
        return new StoreClient(storeURL, logger);
    }

    public static StoreClient getStoreClient(String storeURL)
        throws TException
    {
        LoggerInf logger = new TFileLogger(NAME, 10, 10);
        return new StoreClient(storeURL, logger);
    }

    protected StoreClient(
            String storeURLS,
            LoggerInf logger)
        throws TException
    {
        if (StringUtil.isEmpty(storeURLS)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Missing storeURL");
        }
        try {
            storeURL = new URL(storeURLS);
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Invalid storeURL");
        }
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Missing logger");
        }
        this.storeURL = storeURL;
        this.logger = logger;
    }

    public String getNodeState (
            String format,
            int nodeID)
        throws TException
    {
        testStoreURL();
        testFormat(format);
        ValidateCmdParms.testNodeID(nodeID);
        String requestURL = "state"
                + "/" + nodeID
                + "?" + KeyNameHttpInf.RESPONSEFORM + "=" + format;
        requestURL = storeURL.toString() + "/" + requestURL;
        if (debug) System.out.println("***restURL=" + requestURL);
        return getFormattedString(requestURL);
    }


    public String addVersion (
            String format,
            int nodeID,
            Identifier objectID,
            File manifestFile)
        throws TException
    {
        testStoreURL();
        testFormat(format);
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        if (debug) System.out.println(MESSAGE + "addVersion entered");
        return sendAddVersion(format, nodeID, objectID, manifestFile, 1200000);
    }

    public String getObjectState (
            String format,
            int nodeID,
            Identifier objectID)
        throws TException
    {
        testStoreURL();
        testFormat(format);
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        String requestURL = "state"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue())
                + "?" + KeyNameHttpInf.RESPONSEFORM + "=" + format;
        requestURL = storeURL.toString() + "/" + requestURL;
        if (debug) System.out.println("***restURL=" + requestURL);
        return getFormattedString(requestURL);
    }

    public String getVersionContent (
            String format,
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(
                MESSAGE + "not implemented at this time");
    }

    public String getVersionState (
            String format,
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {

        testStoreURL();
        testFormat(format);
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        String requestURL = "state"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue())
                + "/" + versionID
                + "?" + KeyNameHttpInf.RESPONSEFORM + "=" + format;
        requestURL = storeURL.toString() + "/" + requestURL;
        return getFormattedString(requestURL);
    }

    public FileContent getFile (
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        testStoreURL();
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        ValidateCmdParms.testVersionID(versionID);
        ValidateCmdParms.testFileName(fileName);
        String requestURL = "content"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue())
                + "/" + versionID
                + "/" + uEncode(fileName);
        requestURL = storeURL.toString() + "/" + requestURL;
        if (debug) System.out.println("***restURL=" + requestURL);
        File returnObject = FileUtil.url2TempFile(logger, requestURL);
        FileContent fileContent = FileContent.getFileContent(returnObject, logger);
        return fileContent;
    }



    /**
     * Build call with serialization response
     * Get response into object
     * Return object
     * @param objectID Object Identifier
     * @param versionID Version Identifier
     * @param fileName name of saved file
     * @return
     * @throws org.cdlib.mrt.utility.TException
     */
    public String getFileState (
            String format,
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        testStoreURL();
        testFormat(format);
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        ValidateCmdParms.testVersionID(versionID);
        ValidateCmdParms.testFileName(fileName);
        String requestURL = "state"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue())
                + "/" + versionID
                + "/" + uEncode(fileName)
                + "?" + KeyNameHttpInf.RESPONSEFORM + "=" + format;
        requestURL = storeURL.toString() + "/" + requestURL;
        if (debug) System.out.println("***restURL=" + requestURL);
        return getFormattedString(requestURL);
    }
    /**
     * Build call with serialization response
     * Get response into object
     * Return object
     * @param objectID Object Identifier
     * @param versionID Version Identifier
     * @param fileName name of saved file
     * @return
     * @throws org.cdlib.mrt.utility.TException
     */
    public String getFileFixityState (
            String format,
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        testStoreURL();
        testFormat(format);
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        ValidateCmdParms.testVersionID(versionID);
        ValidateCmdParms.testFileName(fileName);
        String requestURL = "fixity"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue())
                + "/" + versionID
                + "/" + uEncode(fileName)
                + "?" + KeyNameHttpInf.RESPONSEFORM + "=" + format;
        requestURL = storeURL.toString() + "/" + requestURL;
        if (debug) System.out.println("***restURL=" + requestURL);
        return getFormattedString(requestURL);
    }
    public FileContent getVersionArchive(
            int nodeID,
            Identifier objectID,
            int versionID,
            String archiveTypeS)
        throws TException
    {
        testStoreURL();
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        ValidateCmdParms.testVersionID(versionID);
        ValidateCmdParms.testArchiveType(archiveTypeS);
        String requestURL = "content"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue())
                + "/" + versionID
                + "?" + KeyNameHttpInf.RESPONSEFORM + "=" + archiveTypeS
                + "&r=by-value"
                ;
        requestURL = storeURL.toString() + "/" + requestURL;
        if (debug) System.out.println("***restURL=" + requestURL);
        File returnObject = FileUtil.url2TempFile(logger, requestURL);
        FileContent fileContent = FileContent.getFileContent(returnObject, logger);
        return fileContent;
    }


    public FileContent getVersionLink(
            int nodeID,
            Identifier objectID,
            int versionID,
            String linkBaseURL)
        throws TException
    {
        testStoreURL();
        ValidateCmdParms.testNodeID(nodeID);
        ValidateCmdParms.testObjectID(objectID);
        ValidateCmdParms.testVersionID(versionID);
        String requestURL = "content"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue())
                + "/" + versionID
                + "?r=by-reference"
                ;
        requestURL = storeURL.toString() + "/" + requestURL;
        if (debug) System.out.println("***restURL=" + requestURL);
        File returnObject = FileUtil.url2TempFile(logger, requestURL);
        FileContent fileContent = FileContent.getFileContent(returnObject, logger);
        return fileContent;
    }

    /**
     * Send this manifestFile to mrt store
     * @param manifestFile
     * @return
     * @throws org.cdlib.framework.utility.FrameworkException
     */
    protected String sendAddVersion(
            String format,
            int nodeID,
            Identifier objectID,
            File manifestFile,
            int timeout)
        throws TException
    {

        InputStream contents = null;
        try {
            String manifest = FileUtil.file2String(manifestFile, "utf-8");
            Properties prop = new Properties();
            prop.setProperty( "manifest", manifest);
            prop.setProperty( "t", format);
            String requestURL = "add"
                + "/" + nodeID
                + "/" + uEncode(objectID.getValue());
            requestURL = storeURL.toString() + "/" + requestURL;
            contents = HTTPUtil.postObject(requestURL, prop, timeout);
            if (contents != null) {
                return StringUtil.streamToString(contents, "utf-8");
            }
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                    MESSAGE + "Error during sendAddVersion processing");

        } catch( TException tex ) {
            System.out.println("trace:" + StringUtil.stackTrace(tex));
            throw tex;

        } catch( Exception ex ) {
            System.out.println("trace:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "- Exception:" + ex);
        }
    }

    /**
     * Send this manifestFile to mrt store
     * @param manifestFile
     * @return
     * @throws org.cdlib.framework.utility.FrameworkException
     */
    public static String getFormattedString(String requestURL)
        throws TException
    {
        InputStream contents = null;
        try {
            contents = HTTPUtil.getObject(requestURL, 1000, 5);
            return StringUtil.streamToString(contents, "utf-8");

        } catch( TException tex ) {
            System.out.println("trace:" + StringUtil.stackTrace(tex));
            throw tex;

        } catch( Exception ex ) {
            System.out.println("trace:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "- Exception:" + ex);
        }
    }



    protected String uEncode(String in)
        throws TException
    {
        try {
            return URLEncoder.encode(in, "utf-8");

        } catch (Exception ex) {
            throw new TException.INVALID_DATA_FORMAT(
                    "Unable to encode:" + in + " - Exception:" + ex);
        }
    }


    protected void testFormat(String format)
        throws TException
    {
        if (StringUtil.isEmpty(format)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Format not supplied");
        }
        try {
            FormatterInf.Format formatE = FormatterInf.Format.valueOf(format);
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Format not supported:" + format);
        }
    }


    protected void testStoreURL()
        throws TException
    {
        if (storeURL == null) {
            throw CANAbs.setException("remoteCANURL is null");
        }
    }
}

