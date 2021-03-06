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

import org.cdlib.mrt.store.can.CAN;
import org.cdlib.mrt.store.can.CANAbs;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Set;

import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;


/**
 *
 * @author dloy
 */
public class TestCANState
{


    protected static final String NAME = "TestCANState";
    protected static final String MESSAGE = NAME + ": ";
    protected static LoggerInf logger = null;

    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        try
        {
            String propertyList[] = {
                "resources/MFrameDefault.properties",
                "resources/MFrameService.properties",
                "resources/FeederService.properties",
                "resources/IngestClient.properties",
                "resources/FeederClient.properties",
                "resources/MFrameLocal.properties"};

            framework = new TFrame(propertyList, NAME);
            logger = framework.getLogger();
            String manifestName = framework.getProperty(NAME + ".manifestName");
            log("manifest=" + manifestName, 0);
            File manifestFile = new File(manifestName);

            String storeName = framework.getProperty(NAME + ".store");
            log("storeName=" + storeName, 0);
            File storeFile = new File(storeName);
            if (!storeFile.exists()) {
                log("storeFile does not exist", 0);
            }

            String objectIDS = framework.getProperty(NAME + ".objectID");
            log("objectIDS=" + objectIDS, 0);

            String versionIDS = framework.getProperty(NAME + ".versionID");
            log("versionIDS=" + versionIDS, 0);
            int versionID = Integer.parseInt(versionIDS);

            String fileName = framework.getProperty(NAME + ".fileName");
            log("fileName=" + fileName, 0);

            int nodeID =100;
            CAN can = CANAbs.getCAN(logger, storeName);
            Identifier objectID = new Identifier(objectIDS);
            //testAddVersion(can, storeFile, objectID, manifestFile);
            testNodeState(can);
            testGetVersionState(can, objectID, versionID);
            testGetFileState(can, objectID, versionID, fileName);
            testGetFile(can, objectID, versionID, fileName);

        }  catch(Exception e)  {
            if (framework != null)
            {
                framework.getLogger().logError(
                    "Main: Encountered exception:" + e, 0);
                framework.getLogger().logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    public static void testNodeState(
            CAN can)
    {
        try {
            log(MESSAGE + "before bbb testPutVersion", 0);
            NodeState nodeState = can.getNodeState();
            String dump = nodeState.dump("testNodeState");
            log(dump, 0);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }


    public static void testGetVersionState(
            CAN can,
            Identifier objectID,
            int version)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "before testGetVersionState", 0);
            VersionState versionState = can.getVersionState(objectID, version);
            System.out.println("testGetVersionState - " + versionState.dump("testGetVersionState"));

        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    public static void testGetFileState(
            CAN can,
            Identifier objectID,
            int versionID,
            String fileName)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "before testGetFileState", 0);
            FileComponent fileState = can.getFileState(objectID, versionID, fileName);
            System.out.println("testGetFileState - " + fileState.dump("testGetFileState"));

        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    public static void testGetFile(
            CAN can,
            Identifier objectID,
            int versionID,
            String fileName)
    {
        try {
            log("*************************************", 0);
            log(MESSAGE + "before testGetFileState", 0);
            FileContent fileContent = can.getFile (objectID, versionID, fileName);
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


    public static void testAddVersion(
            CAN can,
            File storeFile,
            Identifier objectID,
            File manifestFile)
    {
        try {
            log(MESSAGE + "before ccc testPutVersion", 0);
            VersionState versionState1 = can.addVersion(objectID, null, null, manifestFile);
            dumpVersionState("versionState1",versionState1);
            VersionState versionState2 = can.getVersionState(objectID, 0);
            dumpVersionState("versionState2",versionState2);
            VersionContent versionContent = can.getVersionContent(objectID, 0);
            dumpVersionContent("versionContent",versionContent);
            dumpCANState("CAN State", can);

        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    protected static void dumpVersionContent(String header, VersionContent versionContent)
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

    protected static void dumpVersionState(String header, VersionState versionState)
    {
        log("************dumpVersionState " + header + " ***************", 0);
        log(MESSAGE + versionState.dump("***" + header + "***:"), 0);

    }

    protected static void dumpCANState(String header, CAN can)
        throws TException
    {
        log("************CANSTATE " + header + " ***************", 0);
        NodeState nodeState = can.getNodeState();
        log(nodeState.dump("*******TestCAN********"), 0);

    }

    protected static void log(String msg, int lvl)
    {
        System.out.println(msg);
        logger.logMessage(msg, 0, true);
    }

}