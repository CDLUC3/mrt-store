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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;



import org.cdlib.mrt.store.can.CAN;
import org.cdlib.mrt.store.can.CANAbs;
import org.cdlib.mrt.store.can.CANVirtual;
import org.cdlib.mrt.core.PingState;
import org.cdlib.mrt.store.can.NodeForm;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.StringUtil;

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

    protected static final String NAME = "StoreNodeManager";
    protected static final String MESSAGE = NAME + ": ";
    protected final static boolean DEBUG = false ;

    protected LoggerInf logger = null;
    protected Properties conf = null;
    protected Integer defaultNode = null;
    protected URL storeLink = null;
    protected Hashtable<Integer, StoreNode> m_canTypes = new Hashtable<Integer, StoreNode>(200);
    protected Properties storeProperties = null;
    protected SpecScheme storageServiceSpec = null;
    protected PingState pingState = null;
    protected ArrayList<Integer> virtual = new ArrayList<>();
    
    protected StoreNodeManager(LoggerInf logger, Properties conf)
        throws TException
    {
        this.logger = logger;
        this.conf = conf;
        init(conf);
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
    public static StoreNodeManager getStoreNodeManager(LoggerInf logger, Properties conf)
        throws TException
    {
        try {
            StoreNodeManager nodeManager = new StoreNodeManager(logger, conf);
            return nodeManager;

        } catch (Exception ex) {
            String msg = MESSAGE + "NodeManager Exception:" + ex;
            logger.logError(msg, LoggerInf.LogLevel.SEVERE);
            logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex),
                    LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }

    /**
     * <pre>
     * Initialize the StoreNodeManager
     * Uses the following Properties:
     * StorageService= is File pointer to this Storage Service
     *
     * The startup chain is:
     * StorageService= - gets to the storage service base directory "store"
     * store -> nodes.txt - that contains a file or url pointer to each node supported
     *  by this store
     * store -> store-info.txt - primarily the Access-uri is used in the  getVersion link
     *  manifest
     * </pre>
     * @param prop system properties used to resolve Node references
     * @throws TException process exceptions
     */
    public void init(Properties prop)
        throws TException
    {
        try {
            if (prop == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Exception TFrame properties not set");
            }
            String storageFileS = prop.getProperty("StorageService");
            if (storageFileS != null) {
                if (DEBUG) System.out.println(MESSAGE + "init - process StorageService:" + storageFileS);
                File storageFile = new File(storageFileS);
                if (!storageFile.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "StorageService property not found");
                }
                File nodeTxt = new File(storageFile, "nodes.txt");
                if (!nodeTxt.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "StorageService nodes.txt not found");
                }
                setNodes(nodeTxt);


                File storeInfoTxt = new File(storageFile, "store-info.txt");
                if (!storeInfoTxt.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "StorageService store-info.txt not found");
                }
                storeProperties = PropertiesUtil.loadFileProperties(storeInfoTxt);
                setStorageLink(storeProperties);
                setStorageSpec(storageFile, storeProperties);
            } else {

                String nodeFileS = prop.getProperty("NodeService");
                if (StringUtil.isNotEmpty(nodeFileS)) {
                    if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("NodeService found", prop));
                    defaultNode = setNode(nodeFileS).getIdentifier();
                    storeProperties = prop;
                }
            }
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

    /**
     * Build each of the Node objects (CAN) using the node.txt as a file
     * @param nodeTxt file containing references to all nodes
     * @throws TException processing exception
     */
    public void setNodes(File nodeTxt)
        throws TException
    {
        try {

            FileInputStream fis = new FileInputStream(nodeTxt);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis, "utf-8"));
            String line = null;
            
            int nodeCnt = 0;
            boolean virtualNodeFlag = false;
            while( true ) {
                virtualNodeFlag = false;
                line = br.readLine();
                if (line == null) break;
                if (StringUtil.isAllBlank(line)) continue;
                if (line.substring(0,1).equals("#")) continue;
                if (DEBUG) System.out.println(MESSAGE + "setNodes - node line=" + line);
                try {
                    if (line.startsWith("*")) {
                        line = line.substring(1);
                        virtualNodeFlag = true;
                    }
                    NodeStateInf nodeState = setNode(line);
                    if (virtualNodeFlag) {
                        virtual.add(nodeState.getIdentifier());
                        if (DEBUG) System.out.println("add virtual.size=" + virtual.size());
                    }
                    nodeCnt++;
                } catch (TException.EXTERNAL_SERVICE_UNAVAILABLE esu) { 
                    System.out.println("Node not found:" + line);
                } catch (Exception ex) {
                    System.out.println("************EXCEPTION:" + ex);
                    ex.printStackTrace();
                }
            }
            if (nodeCnt == 0) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "No accessible nodes found");
            }
            //process virtual
            if (DEBUG) System.out.println("virtual.size=" + virtual.size());
            if (virtual.size() > 0) {
                processVirtual();
            }

        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }

    /**
     * For each line in the nodes.txt file
     * Build the can
     * Reference the can object from a table
     * Add any access information used for this node
     * @param line string reference to node
     * @return nodeID node identifier
     * @throws TException processing exception
     */
    public NodeStateInf setNode(String line)
        throws TException
    {
        try {
            NodeInf can = getCan(line);
            int nodeID = can.getNodeID();
            StoreNode storeNode = new StoreNode(can);
            setStoreNode(nodeID, storeNode);


            NodeStateInf nodeState = can.getNodeState();
            String accessURLS = nodeState.getBaseURI();
            if (DEBUG)
                    System.out.println(MESSAGE + "setNode"
                            + " - getBaseURI=" + accessURLS
                            + " - nodeID=" + nodeID
                            );
            if (!StringUtil.isEmpty(accessURLS)) {

                URL urlValue = null;
                try {
                    urlValue = new URL(accessURLS);
                } catch (Exception ex) {
                    throw new TException.INVALID_DATA_FORMAT(MESSAGE + "property value invalid:" + accessURLS);
                }
                storeNode.setNodeLink(urlValue);
            }


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

    /**
     * From the line extracted from the nodes.txt file.
     * Create a local (file reference) or remote (http) CAN
     * @param line node reference
     * @return instantiated node
     * @throws TException process exception
     */
    public NodeInf getCan(String line)
        throws TException
    {
        try {

            NodeInf can = null;
            URL urlValue = null;

            if (line.startsWith("file")) {
                urlValue = null;
                try {
                    urlValue = new URL(line);
                } catch (Exception ex) {
                    throw new TException.INVALID_DATA_FORMAT(MESSAGE + "value invalid:" + line);
                }
                can = setNodeCAN(urlValue);
                int nodeID = can.getNodeID();
                if (DEBUG) {
                    System.out.println(MESSAGE + " - file:"
                            + " - nodeID=" + nodeID
                            + " - urlValue=" + urlValue + "*");
                }
            } else if (line.startsWith("http")) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "remote CAN no longer supported:" + line);
                
            } else {
                can = setRelativeFileNodeCAN(line);
                int nodeID = can.getNodeID();
                if (DEBUG) {
                    System.out.println(MESSAGE + "relative:"
                            + " - nodeID=" + nodeID
                            + " - urlValue=" + line + "*");
                }
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
     * Build a new CAN using a file path
     * @param path file path to storage service node
     * @return CAN
     * @throws Exception process exception
     */
    protected CAN setRelativeFileNodeCAN(String path)
        throws Exception
    {
        LoggerInf localLog = setCANLog(path);
        /**
        System.out.println(MESSAGE + "setNode LOG"
                + " - msgmax=" + localLog.getMessageMaxLevel()
                + " - errmax=" + localLog.getErrorMaxLevel()
                );
         */
        CAN can = CANAbs.getCAN(localLog, path);
        if(DEBUG) System.out.println(MESSAGE + "setRelativeFileNodeCAN"
                + " - nodeID=" + can.getNodeID());
        return can;
    }

    /**
     * If a file URL is used convert to File reference and create a local CAN
     * @param url file URL
     * @return CAN
     * @throws Exception process exception
     */
    protected CAN setNodeCAN(URL url)
        throws Exception
    {
        File nodeFile = FileUtil.fileFromURL(url);
        String path = nodeFile.getAbsolutePath();
        LoggerInf localLog = setCANLog(path);
        /**
        System.out.println(MESSAGE + "setNode LOG"
                + " - msgmax=" + localLog.getMessageMaxLevel()
                + " - errmax=" + localLog.getErrorMaxLevel()
                );
         */
        CAN can = CANAbs.getCAN(localLog, path);
        return can;
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
     * set local logger to node/log/...
     * @param path String path to node
     * @return Node logger
     * @throws Exception process exception
     */
    protected LoggerInf setCANLog(String path)
        throws Exception
    {
        if (StringUtil.isEmpty(path)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "setCANLog: path not supplied");
        }

        File canFile = new File(path);
        File log = new File(canFile, "log");
        if (!log.exists()) log.mkdir();
        LoggerInf logger = LoggerAbs.getTFileLogger("CAN", log.getCanonicalPath() + '/', conf);
        return logger;
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
            StorageServiceState storageState = StorageServiceState.getStorageServiceState(storeProperties);
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
                        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
                    }
                    storageState.addNodeState(nodeState);
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
        return storeLink;
    }
    
    public Map<Integer, StoreNode> getNodeMap() {
        return m_canTypes;
    }
    
    /**
     * Note that for virtual node:
     * the input CAN is a standard CAN that is replaced with a virtual CAN
     * baseURI points to the sourceNode baseURI
     * sourceNode is node identifier for source node
     * targetNode is node identifier for target node
     * @throws TException 
     */
    protected void processVirtual()
        throws TException
    {
        try {
            System.out.println("processVirtual entered");
            for (Integer vnode : virtual) {
                setVirtualCan(vnode);
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void setVirtualCan(int vnode)
        throws TException
    {
        try {
   
            StoreNode storeNode = getStoreNode(vnode);
            NodeInf virtualNode = storeNode.getCan();
            NodeState virtualState = virtualNode.getNodeState();
            String storageBase = virtualState.baseURI;
            if (StringUtil.isAllBlank(storageBase)) {
                throw new TException.INVALID_ARCHITECTURE("baseURI not supplied - node invalid");
            }
            Integer sourceNodeID = virtualState.getSourceNodeID();
            if ((sourceNodeID == null) || (sourceNodeID < 1)) {
                throw new TException.INVALID_ARCHITECTURE("source node not supplied - node invalid");
            }
            StoreNode storeNodeSource = getStoreNode(sourceNodeID);
            if (storeNodeSource == null) {
                throw new TException.INVALID_ARCHITECTURE("storeNodeSource not found");
            }
            NodeInf sourceNode = storeNodeSource.getCan();
            
            Integer targetNodeID = virtualState.getTargetNodeID();
            if ((targetNodeID == null) || (targetNodeID < 1)) {
                throw new TException.INVALID_ARCHITECTURE("target node not supplied - node invalid");
            }
            StoreNode storeNodeTarget = getStoreNode(targetNodeID);
            if (storeNodeTarget == null) {
                throw new TException.INVALID_ARCHITECTURE("storeNodeTarget not found");
            }
            if (virtualState.getNodeForm() != "virtual") {
                virtualState.setNodeForm("virtual");
            }
                        
            NodeInf targetNode = storeNodeTarget.getCan();
            LoggerInf thisLogger = virtualNode.getLogger();
            

            CANVirtual canVirtual = CANVirtual.getCANVirtual(virtualState, sourceNode, targetNode, storageBase, thisLogger);
            
            storeNode.setCan(canVirtual);
            if (DEBUG) System.out.print("Virtual CAN created:"
                    + " - storageBase=" + storageBase
                    + " - nodeID=" + vnode
                    + " - sourceNodeID=" + sourceNodeID
                    + " - targetNodeID=" + targetNodeID
                    + " - sourceCANID=" + sourceNode.getNodeID()
                    + " - targetCANID=" + targetNode.getNodeID()
                    );
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public Properties getStoreProperties() {
        return storeProperties;
    }

    public void setStoreProperties(Properties storeProperties) {
        this.storeProperties = storeProperties;
    }
}

