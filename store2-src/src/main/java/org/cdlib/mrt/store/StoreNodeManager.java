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
package org.cdlib.mrt.store;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



import org.cdlib.mrt.store.can.CANAbs;
import org.cdlib.mrt.core.PingState;
import org.cdlib.mrt.tools.YamlParser;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.store.storage.StorageService;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.StringUtil;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Basic manager for Node handling in the Storage Service
 *
 * <pre>
 * This routine extracts Storage info from store-info.txt and related nodes from nodes.txt
 * This class is used by both the Storage and CAN services
 * </pre>
 * @author dloy
 */
public class StoreNodeManager
{

    private static final Logger log4j = LogManager.getLogger(StoreNodeManager.class.getName());
    
    protected static final String NAME = "StoreNodeManager";
    protected static final String MESSAGE = NAME + ": ";
    protected final static boolean DEBUG = false ;

    protected File storageDir = null;
    protected LoggerInf logger = null;
    //protected Properties conf = null;
    protected Integer defaultNode = null;
    protected URL storeLink = null;
    protected Hashtable<Integer, StoreNode> m_canTypes = new Hashtable<Integer, StoreNode>(200);
    //protected Properties storeProperties = null;
    protected SpecScheme storageServiceSpec = null;
    protected PingState pingState = null;
    protected ArrayList<Integer> virtual = new ArrayList<>();
    protected StorageConfig storageConfig = null;
    //protected NodeIO nodeIO = null;
    
    protected StoreNodeManager(LoggerInf logger, StorageConfig storageConfig)
        throws TException
    {
        this.logger = logger;
        this.storageConfig = storageConfig;
        System.out.println(storageConfig.dump("***StorageNodeManager***"));
        initEnv(storageConfig);
    }

    /**
     * Factory that returns the StoreNodeManager
     * @param logger non node logger
     * @param conf runtime properties file:
     * StorageService= is File pointer to this Storage Service
     * NodeService= is File pointer to this Node Service
     * @return
     * @throws TException
     */
    public static StoreNodeManager getStoreNodeManager(LoggerInf logger, StorageConfig storageConfig)
        throws TException
    {
        try {
            StoreNodeManager nodeManager = new StoreNodeManager(logger, storageConfig);
            return nodeManager;

        } catch (Exception ex) {
            String msg = MESSAGE + "NodeManager Exception:" + ex;
            logger.logError(msg, LoggerInf.LogLevel.SEVERE);
            logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex),
                    LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }
    
  
    private void initEnv(StorageConfig storageConfig)
        throws TException
    {
        try {
            if (DEBUG) System.out.println("***init enetered");
            if (storageConfig == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Exception TFrame properties not set");
            }
            
            List<NodeIO.AccessNode> accessNodes = storageConfig.getAccessNodes();
            setNodes(accessNodes);
            
            if (getNodeCount() == 0) {
                 throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "No node found");
            }
            pingState = new PingState("storage");

        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            if (DEBUG) {
                System.out.println(msg);
                System.out.println(StringUtil.stackTrace(ex));
            }
        }
    }
    
    private static void addProp(String key, JSONObject json, Properties prop)
        throws TException
    {
        String result = null;
        try {
            result = json.getString(key);
        } catch (Exception ex) {
            result = null;
        }
        if (result != null) {
            prop.setProperty(key, result);
        }
    }
    
    private static void addBoolProp(String key, JSONObject json, Properties prop)
        throws TException
    {
        Boolean result = null;
        try {
            result = json.getBoolean(key);
        } catch (Exception ex) {
            result = null;
        }
        if (result != null) {
            prop.setProperty(key, "" + result);
        }
    }
    
    private static void addLongProp(String key, JSONObject json, Properties prop)
        throws TException
    {
        Boolean result = null;
        try {
            result = json.getBoolean(key);
        } catch (Exception ex) {
            result = null;
        }
        if (result != null) {
            prop.setProperty(key, "" + result);
        }
    }
  
    
    /**
     * Find the Storage link, which is contained as the Access-uri property in
     * store-info.txt
     * @param localProp Properties object built from store-info.txt
     * @throws TException  process exception
     */
    protected void setStorageLink(Properties localProp)
        throws TException
    {

        String accessURLS = localProp.getProperty(StorageServiceStateInf.BASEURI);
        if (DEBUG) {
            System.out.println("!!!!storeLink:" + accessURLS);

        if (StringUtil.isEmpty(accessURLS)) return;            }
        URL urlValue = null;
        try {
            urlValue = new URL(accessURLS);
        } catch (Exception ex) {
            throw new TException.INVALID_DATA_FORMAT(MESSAGE + "property value invalid:" + accessURLS);
        }
        this.storeLink = urlValue;
    }
    
    protected void setStorageSpec(File storeDir, Properties localProp)
        throws TException
    {
        String storeSpecS = localProp.getProperty(StorageServiceStateInf.SERVICESCHEME);
        if (StringUtil.isEmpty(storeSpecS)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                MESSAGE + "StorageService " +  StorageServiceStateInf.SERVICESCHEME
                + " not found");
        }
        storageServiceSpec = SpecScheme.buildSpecScheme("store", storeSpecS);
        if (storageServiceSpec == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                MESSAGE + "StorageService " +  StorageServiceStateInf.SERVICESCHEME
                + " spec not found");
        }
        if (DEBUG) {
            System.out.println("!!!!storageServiceSpec:"
                    + storageServiceSpec.getFormatSpec()
                    + " - storeSpecS=" + storeSpecS);
        }
        storageServiceSpec.buildNamasteFile(storeDir);
    }

    public void setStorageLink(URL storeLink)
        throws TException
    {
        this.storeLink = storeLink;
    }
    
    public void setNodes(List<NodeIO.AccessNode> accessNodes)
        throws TException
    {
        try {

            
            int nodeCnt = 0;
            for (NodeIO.AccessNode accessNode : accessNodes) {
                if (accessNode.nodeDescription.startsWith("##")) continue;
                try {
                    NodeState nodeState = setNode(accessNode);
                    nodeCnt++;
                } catch (TException.EXTERNAL_SERVICE_UNAVAILABLE esu) { 
                    System.out.println("Node not found:" + accessNode.nodeNumber);
                } catch (Exception ex) {
                    System.out.println("************EXCEPTION:" + ex);
                    ex.printStackTrace();
                }
            }
            if (nodeCnt == 0) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "No accessible nodes found");
            }

        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
    
    public NodeState setNode(NodeIO.AccessNode accessNode)
        throws TException
    {
        try {
            NodeInf can = getCan(accessNode);
            int nodeID = can.getNodeID();
            StoreNode storeNode = new StoreNode(can);
            setStoreNode(nodeID, storeNode);


            NodeState nodeState = can.getNodeState();
            String accessURLS = nodeState.getBaseURI();
            if (DEBUG)
                    System.out.println(MESSAGE + "setNode"
                            + " - getBaseURI=" + accessURLS
                            + " - nodeID=" + nodeID
                            );
            
            log4j.debug("setNode processing"
                            + " - getBaseURI=" + accessURLS
                            + " - nodeID=" + nodeID
                            );

            return nodeState;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            if (DEBUG) ex.printStackTrace();
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }

    public NodeInf getCan(NodeIO.AccessNode accessNode)
        throws TException
    {
        try {

            NodeInf can = null;

            can = CANAbs.getCAN(logger, accessNode, storageConfig);
            if(DEBUG) System.out.println(MESSAGE + "setRelativeFileNodeCAN"
                + " - nodeID=" + can.getNodeID());
            int nodeID = can.getNodeID();
            if (DEBUG) {
                System.out.println(MESSAGE + "relative:"
                        + " - nodeID=" + nodeID
                        + " - urlValue=" + accessNode.nodeNumber + "*");
            }
            return can;
            
        } catch (TException tex) {
            throw tex;


        } catch (Exception ex) {
            if (DEBUG) ex.printStackTrace();
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }


    /**
     * Save entry to a table for lookup during processing
     * If entry already exists then throw an exception
     * @param nodeID node identifier of CAN
     * @param node CAN reference
     * @throws TException process exception
     */
    public void setStoreNode(int nodeID, StoreNode node)
        throws TException
    {
        StoreNode test = m_canTypes.get(nodeID);
        if (test != null) {
            throw new TException.INVALID_CONFIGURATION(
                    MESSAGE + "Duplicate entries in configuration for NODE");

        }

        if (DEBUG)
            System.out.println(MESSAGE + "setStoreNode"
                    + " - nodeID=" + nodeID
                    + " - node=" + node.getCan().getNodeID()
                    );
        m_canTypes.put(nodeID, node);
    }


    /**
     * Get CAN pointer based on nodeID
     * @param nodeID node identifier
     * @return CAN with CAN info
     */
    public StoreNode getStoreNode(int nodeID)
    {
        return m_canTypes.get(nodeID);
    }


    /**
     * Get count of number of active Nodes
     * @return number of active nodes
     */
    public int getNodeCount()
    {
        return m_canTypes.size();
    }

    /**
     * Get the default nodeID for defining what the current Node service
     * references
     * @return default nodeID
     */
    public Integer getDefaultNodeID()
    {
        return this.defaultNode;
    }

    /**
     * Build a StorageState primarily from store-info.txt combined with status-summary.txt for
     * each node
     * @return Storage Service State
     * @throws TException
     */
    public StorageServiceState getStorageState()
        throws TException
    {
        try {
            if (DEBUG) System.out.println(storageConfig.dump("***getStorageState"));
            StorageServiceState storageState = StorageServiceState.getStorageServiceState(storageConfig);
            StoreNode storeNode = null;
            int nodeCnt = 0;
            for (Integer nodeID : m_canTypes.keySet()) {
                storeNode = m_canTypes.get(nodeID);
                try {
                    NodeState nodeState = storeNode.getCan().getNodeState();
                    Boolean ok = nodeState.getOk();
                    if ((ok != null) && !ok) {
                        String msg = "Test state fails:" + nodeID;
                        System.out.println(msg);
                        storageState.addNodeState(nodeState, false);
                        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
                    }
                    storageState.addNodeState(nodeState, true);
                    nodeCnt++;
                } catch (TException.EXTERNAL_SERVICE_UNAVAILABLE esu) {
                    System.out.println("Node not found:" + nodeID);
                }
            }
            if (nodeCnt == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("No working storage nodes found");
            }
            return storageState;

        } catch (TException me) {
            throw me;

        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);

        }
    }
    
    /**
     * Return a runtime ping state
     * @param doGC perform Garbage Collection before saving properties
     * @return PingState
     * @throws TException 
     */
    public PingState getPingState(boolean doGC)
        throws TException
    {
        try {
            pingState.set(doGC);
            return pingState;

        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);

        }
    }
    
    public void bumpCmdTally(String key, long startTime)
    {
        pingState.bumpCmd(key, startTime);
    }

    /**
     * get the store link - store-info.txt Access-uri value
     * @return store link used in getVersion manifest
     */
    public URL getStoreLink() {
        return storageConfig.getStoreLink();
    }
    
    public Map<Integer, StoreNode> getNodeMap() {
        return m_canTypes;
    }
    
    public static void main(String[] args) throws Exception {
        String storeInfoYmlS = "/apps/replic/tst/config/storage/mrtHomes/storage/store-info.yml";
        File storeInfoYmlFile = new File(storeInfoYmlS);
        String yaml = FileUtil.file2String(storeInfoYmlFile);
        System.out.println("YAML:" + yaml);
                YamlParser yamlParser = new YamlParser();
                yamlParser.parseString(yaml);
                yamlParser.resolveValues();
                String jsonS = yamlParser.dumpJson();
                System.out.println("JSON:" + jsonS);
    }

    public StorageConfig getStorageConfig() {
        return storageConfig;
    }
    
}

