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
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.CloudStoreInf;
//import org.cdlib.mrt.store.ObjectLocationInf;
//import org.cdlib.mrt.store.ObjectLocationAbs;
import org.cdlib.mrt.store.ObjectStoreInf;
import org.cdlib.mrt.store.ObjectStoreAbs;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.PreSignedState;
import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.store.cloud.CANCloudService;
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
    //protected ObjectLocationInf objectLocation;
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
            //ObjectLocationInf objectLocation,
            ObjectStoreInf objectStore,
            NodeState nodeState)
    {
        this.logger = logger;
        this.nodeForm = nodeForm;
        //this.objectLocation = objectLocation;
        this.objectStore = objectStore;
        this.nodeState = nodeState;
    }
    
    protected void copyAdmin(CANAbs canAbs)
    {
        this.nodeForm = canAbs.nodeForm;
        //this.objectLocation =  canAbs.objectLocation;
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
    

    public static CANCloud getCAN(LoggerInf logger, 
            NodeIO.AccessNode accessNode,  
            StorageConfig storeConfig)
            throws TException
    {
        

        try{
            NodeForm nodeForm = setNode(accessNode, storeConfig, logger);
            if (nodeForm == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "NodeForm not found");
            }
            
            CANCloud can = setCAN(logger, nodeForm, accessNode);
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
    
    protected static CANCloud setCAN(
            LoggerInf logger,
            NodeForm nodeForm,
            NodeIO.AccessNode accessNode)
        throws TException
    {
        /*
        ObjectLocationInf objectLocation = null;
        ObjectStoreInf objectStore = null;
        try {
            if (nodeForm.getObjectLocationType().equals("pairtree")) {
                objectLocation = ObjectLocationAbs.getPairTree(
                        logger,
                        nodeForm.getCanHomeStore());
            } else if (nodeForm.getObjectLocationType().equals("cloud")) {
                if (false) objectLocation = ObjectLocationAbs.getDummyCloud(
                        logger,
                        nodeForm.getCanHomeStore());
            }

        } catch (TException mfex) {
            logger.logError(StringUtil.stackTrace(mfex), 10);
            throw mfex;
        }
        */
        String objectStoreType = nodeForm.getObjectStoreType();
        if (StringUtil.isEmpty(objectStoreType)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectStoreType missing");
        }
        NodeState nodeState = nodeForm.getNodeState();

        if (true) System.out.println(MESSAGE + "setCAN - before new CAN"
                + " - nodeNumber=" + accessNode.nodeNumber
                + " - nodeDescription=" + accessNode.nodeDescription
                + " - isVerifyOnRead=" + nodeState.isVerifyOnRead()
                + " - isVerifyOnWrite=" + nodeState.isVerifyOnWrite()
        );
   
        CANCloudService canCloudService = CANCloudService.getCANCloudService(accessNode, 
                nodeState.isVerifyOnRead(), nodeState.isVerifyOnWrite(), logger);
        CANCloud can = CANCloud.getCANCloud(logger,nodeForm, canCloudService, nodeState);
        can.setNodeID(nodeState.getIdentifier());

        if (DEBUG) System.out.println(MESSAGE + "setCAN - after new CAN"
                + " - nodeID=" + can.getNodeID());
        return can;
    }

    
    public PreSignedState cloudPreSignFile(
            String key,
            long expireMinutes,
            String contentType,
            String contentDisp
    )
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE("cloudPreSignFile - Not supported");
    }

    /**
     * Using node properties construct the NodeForm
     * @param nodeID node identifier
     * @param nodePath path to node properties
     * @return resolved NodeForm
     * @throws Exception
     */
    protected static NodeForm setNode(NodeIO.AccessNode accessNode, StorageConfig storageConfig, LoggerInf logger)
        throws Exception
    {
        NodeForm nodeForm = new NodeForm();
        setCANForm(accessNode, nodeForm, storageConfig, logger);

        if (DEBUG) System.out.println(MESSAGE + "setNode-"
                + " - nodeID=" + nodeForm.getNodeID());
        return nodeForm;


    }

    protected static void setCANForm(
            NodeIO.AccessNode accessNode,
            NodeForm nodeForm,
            StorageConfig storageConfig,
            LoggerInf logger)
        throws Exception
    {
        NodeState nodeState = new NodeState(accessNode, storageConfig, logger);
        int nodeID = nodeState.getIdentifier();
        if (DEBUG) System.out.println(MESSAGE + "setCANForm-"
                + " - nodeID=" + nodeID
        //        + " - " + PropertiesUtil.dumpProperties("", canProp, 50)
                );
        nodeForm.setNodeState(nodeState);
        nodeForm.setNodeID(nodeID);
        String protocol = nodeState.getAccessProtocol();

        if ((protocol == null) || !protocol.toLowerCase().contains("s3")) {
            
            
        } else if (protocol.toLowerCase().contains("virtual")) {
            nodeForm.setObjectLocationType("virtual");
            nodeForm.setObjectStoreType(protocol);
            
        } else {
            nodeForm.setObjectLocationType("cloud");
            nodeForm.setObjectStoreType(protocol);
        }
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

