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
**********************************************************/
package org.cdlib.mrt.store.can;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.store.ObjectLocationInf;
import org.cdlib.mrt.store.ObjectLocationAbs;
import org.cdlib.mrt.store.ObjectStoreInf;
import org.cdlib.mrt.store.ObjectStoreAbs;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.store.je.LocalIDDatabase;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Base CAN handling
 * @author dloy
 */
public abstract class CANAbs
{
    protected static final String NAME = "CANAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected final static String NL = System.getProperty("line.separator");

    protected LoggerInf logger = null;
    protected ObjectLocationInf objectLocation;
    protected ObjectStoreInf objectStore;
    protected int nodeID = 0;
    protected NodeForm nodeForm;
    protected NodeState nodeState;
    protected URL remoteCANURL = null;
    protected static boolean DEBUG = false;

    /**
     *  Constructor for a local CAN
     * @param logger process logging
     * @param nodeID node identifier
     * @param nodeForm contains basic node (CAN) information
     * @param objectLocation handler for directory type index to storage leaf (e.g. pairtree)
     * @param objectStore storage leaf handler (e.g. dflat)
     * @param nodeState static nodeStat information
     */
    protected CANAbs(
            LoggerInf logger,
            NodeForm nodeForm,
            ObjectLocationInf objectLocation,
            ObjectStoreInf objectStore,
            NodeState nodeState)
    {
        this.logger = logger;
        this.nodeForm = nodeForm;
        this.objectLocation = objectLocation;
        this.objectStore = objectStore;
        this.nodeState = nodeState;
    }
    
    protected void copyAdmin(CANAbs canAbs)
    {
        this.nodeForm = canAbs.nodeForm;
        this.objectLocation =  canAbs.objectLocation;
        this.objectStore =  canAbs.objectStore;
        this.nodeState =  canAbs.nodeState;
    }

    /**
     * Constructor for a remote CAN
     * @param nodeID node identifier
     * @param remoteCANURL URL to remote can
     * @param logger local process log
     */
    protected CANAbs(
            URL remoteCANURL,
            LoggerInf logger)
    {
        this.remoteCANURL = remoteCANURL;
        this.logger = logger;
    }

    /**
     * Get CAN process log
     * @return
     */
    public LoggerInf getLogger() {
        return logger;
    }

    /**
     * Get URL to remote CAN
     * @return remote CAN URL
     */
    protected URL getRemoteCANURL() {
        return remoteCANURL;
    }

    /**
     * Get static Node Stat infor this CAN
     * @return
     */
    public NodeState getCanNodeState()
    {
        return nodeState;
    }

    /**
     * CAN factory - local CAN
     * @param logger local process log
     * @param path file path to local can-info.txt properties
     * @return CAN
     * @throws TException process excepton
     */
    public static CAN getCAN(LoggerInf logger, String path)
            throws TException
    {
        if (StringUtil.isEmpty(path)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Missing path in ObjectStoreFactory");
        }
        //File storeFile = null;
        try {
            File storeFile = new File(path);
            if (!storeFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    "File Path not resoved");
            }
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to resolve ObjectStore base: Exception" + ex);
        }

        try{
            NodeForm nodeForm = setNode(path);
            if (nodeForm == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "NodeForm not found");
            }
            int nodeID = nodeForm.getNodeID();
            CAN can = setCAN(logger, nodeID, nodeForm);
            if (DEBUG) System.out.println(MESSAGE + "getCAN"
                    + " - nodeID=" + can.getNodeID()
                    );
            return can;

        } catch (Exception ex) {
            logger.logError(StringUtil.stackTrace(ex), 10);
            ex.printStackTrace();
            throw new TException.INVALID_OR_MISSING_PARM(
                "Unable to instantiate ObjecStore: Exception" + ex, ex);
        }
    }

    /**
     * Build CAN based on can-info.txt properties
     * @param logger process logger
     * @param nodeID node identifier
     * @param nodeForm node form
     * @return constructed CAN
     * @throws TException
     */
    protected static CAN setCAN(
            LoggerInf logger,
            int nodeID,
            NodeForm nodeForm)
        throws TException
    {
        ObjectLocationInf objectLocation = null;
        ObjectStoreInf objectStore = null;
        try {
            if (nodeForm.getObjectLocationType().equals("pairtree")) {
                objectLocation = ObjectLocationAbs.getPairTree(
                        logger,
                        nodeForm.getCanHomeStore());
            } else if (nodeForm.getObjectLocationType().equals("cloud")) {
                objectLocation = ObjectLocationAbs.getDummyCloud(
                        logger,
                        nodeForm.getCanHomeStore());
            }

        } catch (TException mfex) {
            logger.logError(StringUtil.stackTrace(mfex), 10);
            throw mfex;
        }
        String objectStoreType = nodeForm.getObjectStoreType();
        if (StringUtil.isEmpty(objectStoreType)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectStoreType missing");
        }
        NodeState nodeState = nodeForm.getNodeState();
        try {
                objectStore = ObjectStoreAbs.getObjectStore(logger, nodeState);
    
        } catch (TException mfex) {
            throw mfex;
        }

        if (DEBUG) System.out.println(MESSAGE + "setCAN - before new CAN"
                + " - nodeID=" + nodeID);
        setLocalIDDb(logger, nodeForm);
        CAN can = new CAN(logger, nodeForm, objectLocation, objectStore, nodeState);
        can.setNodeID(nodeState.getIdentifier());

        if (DEBUG) System.out.println(MESSAGE + "setCAN - after new CAN"
                + " - nodeID=" + can.getNodeID());
        return can;
    }

    protected static void setLocalIDDb(
            LoggerInf logger,
            NodeForm nodeForm)
        throws TException
    {
        try {
            File canHome = nodeForm.getCanHome();
            File localIDDbDir = new File(canHome, "admin/idmap");
            if (!localIDDbDir.exists()) {
                localIDDbDir.mkdirs();
            }
            LocalIDDatabase localIDDatabase
                = LocalIDDatabase.getLocalIDDatabase(logger, localIDDbDir, false);
            nodeForm.setLocalIDDb(localIDDatabase);

        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION(ex);
        }

    }

    public LocalIDDatabase getLocalIDDb()
    {
        return nodeForm.getLocalIDDb();
    }

    /**
     * Using node properties construct the NodeForm
     * @param nodeID node identifier
     * @param nodePath path to node properties
     * @return resolved NodeForm
     * @throws Exception
     */
    protected static NodeForm setNode(String nodePath)
        throws Exception
    {
        NodeForm nodeForm = new NodeForm();
        if (nodeForm == null) return nodeForm;
        File nodeHome = new File(nodePath);
        if (!nodeHome.exists()) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "setNode - required directory CAN does not exist:" + nodePath);
        }
        nodeForm.setCanHome(nodeHome);
        File nodeStore = new File(nodeHome, "store");
        if (!nodeStore.exists()) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "setNode - required directory store does not exist:" + nodePath);
        }
        nodeForm.setCanHomeStore(nodeStore);
        Properties canProp = getNodeProperties(nodeHome);
        setCANForm(canProp, nodeForm, nodeHome);

        if (DEBUG) System.out.println(MESSAGE + "setNode-"
                + " - nodeID=" + nodeForm.getNodeID());
        return nodeForm;


    }

    /**
     * Complete CANForm extraction
     * Resolve requested type handlers (e.g. pairtree, dflat)
     * @param nodeID node identifier
     * @param canProp properties info from can-info.txt
     * @param nodeForm partial CANForm
     * @param nodeHome home directory for CAN
     * @throws Exception
     */
    protected static void setCANForm(
            Properties canProp,
            NodeForm nodeForm,
            File nodeHome)
        throws Exception
    {
        System.out.println(PropertiesUtil.dumpProperties("setCANForm", canProp));
        NodeState nodeState = new NodeState(canProp, nodeHome);
        int nodeID = nodeState.getIdentifier();
        if (DEBUG) System.out.println(MESSAGE + "setCANForm-"
                + " - nodeID=" + nodeID
                + " - " + PropertiesUtil.dumpProperties("", canProp, 50)
                );
        nodeForm.setNodeState(nodeState);
        nodeForm.setNodeID(nodeID);
        String protocol = nodeState.getAccessProtocol();

        if ((protocol == null) || !protocol.toLowerCase().contains("s3")) {
            String branch = nodeState.getBranchScheme();
            branch = branch.toLowerCase();
            if (branch.contains("pairtree")) {
                nodeForm.setObjectLocationType("pairtree");
            } else {
                throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "setCANForm - Branch-scheme not recognized:" + branch);
            }

            String leaf = nodeState.getLeafScheme();
            leaf = leaf.toLowerCase();
            if (leaf.contains("dflat")) {
                nodeForm.setObjectStoreType("dflat");
            } else {
                throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "setCANForm - Leaf-scheme not recognized:" + leaf);
            }
            
        } else if (protocol.toLowerCase().contains("virtual")) {
            nodeForm.setObjectLocationType("virtual");
            nodeForm.setObjectStoreType(protocol);
            
        } else {
            nodeForm.setObjectLocationType("cloud");
            nodeForm.setObjectStoreType(protocol);
        }
    }

    /**
     * Get CAN properties
     * @param nodeHome node home directory
     * @return Resolved node properties
     * @throws Exception
     */
    protected static Properties getNodeProperties(File nodeHome)
        throws Exception
    {
        File canInfo = new File(nodeHome, "can-info.txt");
        if (!canInfo.exists()) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "setObjectStore - required directory can-info.txt does not exist:"
                    + nodeHome.getAbsolutePath());
        }
        InputStream inStream = new FileInputStream(canInfo);
        Properties canProp = new Properties();
        canProp.load(inStream);
        return canProp;
    }

    
    protected VersionMap getVersionMap(File versionMapFile)
            throws TException
    {
        try {
            InputStream manifestXMLIn = new FileInputStream(versionMapFile);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found");
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    /**
     * process log
     * @param msg log message
     * @param lvl verbosity level
     */
    protected void log(String msg, int lvl)
    {
        logger.logMessage(msg, lvl, true);
    }

    protected  void testRemoteCANURL()
        throws TException
    {
        if (remoteCANURL == null) {
            throw setException("remoteCANURL is null");
        }
    }

    public static TException setException(String header)
    {
        return new TException.REQUEST_INVALID(
                MESSAGE + header);
    }

    /**
     * get the node identifier
     * @return node identifier
     */
    public int getNodeID()
    {
        return nodeID;
    }

    /**
     * set node identifier
     * @param nodeID node identifier
     */
    public void setNodeID(int nodeID)
    {
        this.nodeID = nodeID;
    }
    
    public static boolean isTempCldFile(File inFile)
        throws TException
    {
        boolean isTemp = false;
        try {
            String path = inFile.getAbsolutePath();
            //if (path.matches(".*\\/tmp[0123456789]+\\.txt")) return true;
            if (path.matches(".*\\/tmpcld\\.[0123456789]+.*\\.txt")) {
                isTemp = true;
            } else {
                isTemp = false;
            }
            if (DEBUG) System.out.println("isTempFile: " + path + " - isTemp=" + isTemp);
            return isTemp;
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
}

