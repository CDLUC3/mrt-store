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
package org.cdlib.mrt.store.test;

import org.cdlib.mrt.store.tools.StoreClient;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;

import org.cdlib.mrt.store.storage.StorageService;
import org.cdlib.mrt.store.storage.StorageServiceAbs;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;


/**
 *
 * @author dloy
 */
public class TestHttpClient
{


    protected static final String NAME = "TestHttpClient";
    protected static final String MESSAGE = NAME + ": ";
    protected LoggerInf logger = null;
    protected TFrame tFrame = null;
    protected File storeDirectory = null;
    protected String storeURL = null;
    protected StoreClient storeClient = null;


    public TestHttpClient(TFrame frame)
    {
        this.tFrame = frame;
        this.logger = frame.getLogger();
    }
    /**
     * Main method
     */
    public static void main(String args[])
    {

            String propertyList[] = {
                "resources/MFrameDefault.properties",
                "resources/MFrameService.properties",
                "resources/FeederService.properties",
                "resources/IngestClient.properties",
                "resources/TFrameStorage.properties",
                "resources/TFrameLocal.properties"};
        try {
                TFrame tFrame = new TFrame(propertyList, "TestSerializable");
                TestHttpClient test = new TestHttpClient(tFrame);
                test.run();

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
        }
    }
    
    protected void run()
    {
        try
        {

            Properties confProp = tFrame.getProperties();
            log(PropertiesUtil.dumpProperties("tFrame", confProp),0);
            String manifestName = tFrame.getProperty(NAME + ".manifestName");
            log("manifest=" + manifestName, 0);
            File manifestFile = new File(manifestName);

            String storeName = tFrame.getProperty(NAME + ".store");
            log("storeName=" + storeName, 0);
            storeDirectory = new File(storeName);
            if (!storeDirectory.exists()) {
                log("storeDirectory does not exist", 0);
            }

            storeURL = tFrame.getProperty(NAME + ".storeURL");
            log("storeURL=" + storeURL, 0);
            storeClient = StoreClient.getStoreClient(storeURL);


            String objectIDS = tFrame.getProperty(NAME + ".objectID");
            log("objectIDS=" + objectIDS, 0);;

            String nodeIDS = tFrame.getProperty(NAME + ".nodeID");
            log("nodeIDS=" + nodeIDS, 0);
            int nodeID = Integer.parseInt(nodeIDS);

            String versionIDS = tFrame.getProperty(NAME + ".versionID");
            log("versionIDS=" + versionIDS, 0);
            int versionID = Integer.parseInt(versionIDS);

            String fileName = tFrame.getProperty(NAME + ".fileName");
            log("fileName=" + fileName, 0);
            Identifier objectID = new Identifier(objectIDS);

            String format = tFrame.getProperty(NAME + ".format");
            log("fileName=" + fileName, 0);

            String addState = testAddVersion(format, nodeID, objectID, manifestFile);
            log(addState, 0);
            System.out.println("addState=****" + addState + "****");
            System.out.println("After testAddVersion*************************");
            //testNodeState(nodeID);
            //testObjectState(nodeID, objectID);
            //testVersionState(nodeID, objectID, versionID);
            //testGetFileState(nodeID, objectID, versionID, fileName);
            //testGetFile(can, objectID, versionID, fileName);

        }  catch(Exception e)  {
            log("Exception:" + e, 0);
            log(StringUtil.stackTrace(e), 0);
        }
    }

    /*

    public void testNodeState(int nodeID)
    {
        try {
            log(MESSAGE + "start testNodeState", 0);
            NodeState nodeState = storageService.getNodeState(nodeID);
            String dump = nodeState.dump("testNodeState");
            log(dump, 0);
            File serialFile = new File(storeDirectory, "nodeState");
            serialize(nodeState, serialFile);
            NodeState deSerial = (NodeState)deserialize(serialFile);
            dump = deSerial.dump("testNodeState");
            log(dump, 0);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }


    public void testVersionState(
            int nodeID,
            Identifier objectID,
            int version)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "start testVersionState", 0);
            VersionState versionState = storageService.getVersionState(nodeID, objectID, version);
            log("testGetVersionState - " + versionState.dump("testGetVersionState"), 0);
            File serialFile = new File(storeDirectory, "versionState");
            serialize(versionState, serialFile);
            VersionState deSerial = (VersionState)deserialize(serialFile);
            String dump = deSerial.dump("testNodeState");
            log(dump, 0);


        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }


    public void testObjectState(
            int nodeID,
            Identifier objectID)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "start testObjectState", 0);
            ObjectState objectState = storageService.getObjectState(nodeID, objectID);
            log("testObjectState - " + objectState.dump("testObjectState"), 0);
            File serialFile = new File(storeDirectory, "objectState");
            serialize(objectState, serialFile);
            ObjectState deSerial = (ObjectState)deserialize(serialFile);
            String dump = deSerial.dump("testObjectState");
            log(dump, 0);


        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    public void testGetFileState(
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "before testGetFileState", 0);
            FileState fileState = storageService.getFileState(nodeID, objectID, versionID, fileName);
            log("testGetFileState - " + fileState.dump("testGetFileState"), 0);


            File serialFile = new File(storeDirectory, "fileState");
            serialize(fileState, serialFile);
            FileState deSerial = (FileState)deserialize(serialFile);
            String dump = deSerial.dump("testGetFileState");
            log(dump, 0);

        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    public void testGetFile(
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "before testGetFileState", 0);
            FileContent fileContent = storageService.getFile (nodeID, objectID, versionID, fileName);
            FileState fileState = fileContent.getFileState();

            System.out.println("testGetFile - " + fileState.dump("testGetFileState"));
            fileDump("testGetFile", fileContent.getFile());

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    protected static void fileDump(String header, File dumpFile)
            throws Exception
    {
        System.out.println(header
                + " - name=" + dumpFile.getName()
                + " - path=" + dumpFile.getCanonicalPath()
                );
    }

    public void testAddVersion(
            int nodeID,
            Identifier objectID,
            File manifestFile)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "start testAddVersionSingle", 0);
            VersionState versionState = sendAddVersion(nodeID, objectID, manifestFile);
            log("testGetVersionState - " + versionState.dump("testGetVersionState"), 0);
            File serialFile = new File(storeDirectory, "versionState");
            serialize(versionState, serialFile);
            VersionState deSerial = (VersionState)deserialize(serialFile);
            String dump = deSerial.dump("testAddVersionSingle");
            log(dump, 0);


        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

*/

    /**
     * Send this manifestFile to mrt store
     * @param manifestFile
     * @return
     * @throws org.cdlib.framework.utility.FrameworkException
     */
    protected String testAddVersion(
            String format,
            int nodeID,
            Identifier objectID,
            File manifestFile)
        throws TException
    {

        try {
            String manifest = FileUtil.file2String(manifestFile, "utf-8");
            return storeClient.addVersion(format, nodeID, objectID, manifestFile);

        } catch( TException tex ) {
            System.out.println("trace:" + StringUtil.stackTrace(tex));
            throw tex;

        } catch( Exception ex ) {
            System.out.println("trace:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "- Exception:" + ex);
        }
    }


    protected void log(String msg, int lvl)
    {
        System.out.println(msg);
        logger.logMessage(msg, 0, true);
    }

}