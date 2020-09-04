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
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;


/**
 *
 * @author dloy
 */
public class TestSerializable
{


    protected static final String NAME = "TestSerializable";
    protected static final String MESSAGE = NAME + ": ";
    protected LoggerInf logger = null;
    protected StorageService storageService = null;
    protected TFrame tFrame = null;
    protected File storeDirectory = null;

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
                TestSerializable test = new TestSerializable(tFrame);
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

            String objectIDS = tFrame.getProperty(NAME + ".objectID");
            log("objectIDS=" + objectIDS, 0);;

            log("conf-nodeIDS:" + confProp.getProperty("TestSerializable.nodeID"), 0);
            String nodeIDS = tFrame.getProperty(NAME + ".nodeID");
            log("nodeIDS=" + nodeIDS, 0);
            int nodeID = Integer.parseInt(nodeIDS);

            String versionIDS = tFrame.getProperty(NAME + ".versionID");
            log("versionIDS=" + versionIDS, 0);
            int versionID = Integer.parseInt(versionIDS);

            String fileName = tFrame.getProperty(NAME + ".fileName");
            log("fileName=" + fileName, 0);
            storageService = StorageServiceAbs.getStorageService(logger, confProp);
            Identifier objectID = new Identifier(objectIDS);
            //testAddVersion(can, storeFile, objectID, manifestFile);
            testNodeState(nodeID);
            testObjectState(nodeID, objectID);
            testVersionState(nodeID, objectID, versionID);
            testGetFileState(nodeID, objectID, versionID, fileName);
            //testGetFile(can, objectID, versionID, fileName);

        }  catch(Exception e)  {
            if (tFrame != null)
            {
                tFrame.getLogger().logError(
                    "Main: Encountered exception:" + e, 0);
                tFrame.getLogger().logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    public TestSerializable(TFrame frame)
    {
        this.tFrame = frame;
        this.logger = frame.getLogger();
    }

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
            FileComponent fileState = storageService.getFileState(nodeID, objectID, versionID, fileName);
            log("testGetFileState - " + fileState.dump("testGetFileState"), 0);


            File serialFile = new File(storeDirectory, "fileState");
            serialize(fileState, serialFile);
            FileComponent deSerial = (FileComponent)deserialize(serialFile);
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
            FileComponent fileState = fileContent.getFileComponent();

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
            File storeFile,
            Identifier objectID,
            File manifestFile)
    {
        try {
            log(MESSAGE + "before ccc testPutVersion", 0);
            VersionState versionState1 = storageService.addVersion(nodeID, objectID, null, null, manifestFile);
            dumpVersionState("versionState1",versionState1);
            VersionState versionState2 = storageService.getVersionState(nodeID, objectID, 0);
            dumpVersionState("versionState2",versionState2);
            VersionContent versionContent = storageService.getVersionContent(nodeID, objectID, 0);
            dumpVersionContent("versionContent",versionContent);
            dumpCANState("CAN State", nodeID);

        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    protected void dumpVersionContent(String header, VersionContent versionContent)
    {
        log("************dumpState " + header + " ***************", 0);
        LinkedHashMap<String, FileComponent> versionFiles = versionContent.getVersionTable();
        Set<String> fileSet = versionFiles.keySet();
        for (String key : fileSet) {
            if (StringUtil.isEmpty(key)) continue;
            FileComponent fileState = versionFiles.get(key);
            if (fileState == null) continue;
            log(MESSAGE + fileState.dump("***" + key + "***:"), 0);
        }

    }

    protected void dumpVersionState(String header, VersionState versionState)
    {
        log("************dumpVersionState " + header + " ***************", 0);
        log(MESSAGE + versionState.dump("***" + header + "***:"), 0);

    }

    protected void dumpCANState(String header, int nodeID)
        throws TException
    {
        log("************CANSTATE " + header + " ***************", 0);
        NodeState nodeState = storageService.getNodeState(nodeID);
        log(nodeState.dump("*******TestCAN********"), 0);

    }

    protected void serialize(Serializable serialObject, File outFile)
        throws TException
    {

        FileOutputStream fos = null;
        ObjectOutputStream out = null;
        try {
            fos = new FileOutputStream(outFile);
            out = new ObjectOutputStream(fos);
            out.writeObject(serialObject);
            out.close();

        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("Exception:" + ex);
        }
    }

    protected Serializable deserialize(File inFile)
        throws TException
    {

        FileInputStream fis = null;
        ObjectInputStream in = null;
        try {
            fis = new FileInputStream(inFile);
            in = new ObjectInputStream(fis);
            Serializable serial = (Serializable)in.readObject();
            in.close();
            return serial;

        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("Exception:" + ex);
        }
    }

    protected void log(String msg, int lvl)
    {
        System.out.println(msg);
        logger.logMessage(msg, 0, true);
    }

}