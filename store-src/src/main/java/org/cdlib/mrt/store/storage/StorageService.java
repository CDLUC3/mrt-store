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
package org.cdlib.mrt.store.storage;

import org.cdlib.mrt.store.NodeInf;

import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.PingState;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.store.FileState;
import org.cdlib.mrt.store.StoreNode;
import org.cdlib.mrt.store.StorageServiceState;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.ObjectFixityState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * StorageService
 * @author dloy
 */
public class StorageService
        extends StorageServiceAbs
        implements StorageServiceInf
{
    protected static final String NAME = "StorageService";
    protected static final String MESSAGE = NAME + ": ";

    private static final boolean DEBUG = false;
    private static final boolean STATS = false;
    private static final boolean CMDSTAT = true;


    protected StorageService(
            LoggerInf logger,
            Properties confProp)
        throws TException
    {
        super(logger, confProp);
    }

    @Override
    public VersionState addVersion (
            int nodeID,
            Identifier objectID,
            String context,
            String localID,
            File manifestFile)
    throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        VersionState versionState = can.addVersion(objectID, context, localID, manifestFile);
        URL storeURL = nodeManager.getStoreLink();
        versionState.setAccess( getAccessNodeID(nodeID), storeURL, objectID);
        bump("addVersion", startTime);
        return versionState;
    }

    @Override
    public VersionState updateVersion (
            int nodeID,
            Identifier objectID,
            String context,
            String localID,
            File manifestFile,
            String [] deleteList)
    throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        VersionState versionState = can.updateVersion(objectID, context, localID, manifestFile, deleteList);
        URL storeURL = nodeManager.getStoreLink();
        versionState.setAccess( getAccessNodeID(nodeID), storeURL, objectID);
        bump("updateVersion", startTime);
        return versionState;
    }


    @Override
    public VersionState deleteVersion (
            int nodeID,
            Identifier objectID,
            int versionID)
    throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        VersionState state = can.deleteVersion(objectID, versionID);
        bump("deleteVersion",startTime);
        return state;
    }

    @Override
    public ObjectState deleteObject (
            int nodeID,
            Identifier objectID)
    throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        ObjectState state = can.deleteObject(objectID);
        bump("deleteObject", startTime);
        return state;
    }

    @Override
    public ObjectState copyObject (
            int nodeID,
            Identifier objectID)
    throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        ObjectState state = can.copyObject(objectID);
        bump("copyObject", startTime);
        return state;
    }

    @Override
    public ObjectState copyObject (
            int sourceNodeID,
            int targetNodeID,
            Identifier objectID)
    throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode sourceNode = nodeManager.getStoreNode(sourceNodeID);
        NodeInf sourceCan = sourceNode.getCan();
        StoreNode targetNode = nodeManager.getStoreNode(targetNodeID);
        NodeInf targetCan = targetNode.getCan();
        URL storageBaseURI = nodeManager.getStoreLink();
        if (storageBaseURI == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "copyObject baseURI is missing");
        }
        String storageBase = storageBaseURI.toString();
        ObjectState state = sourceCan.copyObject(storageBase, objectID, targetCan);
        bump("copyObject", startTime);
        return state;
    }

    @Override
    public StorageServiceState getServiceState()
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StorageServiceState state = nodeManager.getStorageState();
        bump("getServiceState", startTime);
        return state;
    }
    
    @Override
    public PingState getPingState(boolean doGC)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        PingState state = nodeManager.getPingState(doGC);
        bump("getPingState", startTime);
        return state;
    }

    @Override
    public NodeState getNodeState (
            int nodeID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        NodeState state = can.getNodeState();
        bump("getNodeState", startTime);
        return state;
    }

    @Override
    public ObjectState findObjectState (
            Identifier objectID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        Map<Integer, StoreNode> nodeMap = nodeManager.getNodeMap();
        TException saveException = null;
        NodeInf can = null;
        ObjectState objectState = null;
        Integer currentNode = null;
        for (Integer nodeID : nodeMap.keySet()) {
            currentNode = nodeID;
            if (DEBUG) System.out.println(">>>Node" + nodeID);
            saveException = null;
            StoreNode storeNode = nodeMap.get(nodeID);
            can = storeNode.getCan();
            try {
                objectState = can.getObjectState(objectID);
                break;
                
            } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                saveException = rinf;
                if (DEBUG) System.out.println(">>>NotFound:" + rinf);
                continue;
            
            } catch (TException tex) {
                if (DEBUG) System.out.println(">>>TException:" + tex);
                if (tex.toString().contains("REQUESTED_ITEM_NOT_FOUND")) {
                    continue;
                }
                throw tex;
                
            } catch (Exception ex) {
                if (DEBUG) System.out.println(">>>Exception:" + ex);
                throw new TException(ex);
            }
        }
        if (saveException != null) throw saveException;
        URL storeURL = nodeManager.getStoreLink();
        objectState.setAccess(storeURL, getAccessNodeID(currentNode));
        return objectState;
    }

    @Override
    public ObjectState getObjectState (
            int nodeID,
            Identifier objectID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        ObjectState objectState = can.getObjectState(objectID);
        URL storeURL = nodeManager.getStoreLink();
        objectState.setAccess(storeURL, getAccessNodeID(nodeID));
        bump("getObjectState", startTime);
        return objectState;
    }
    
    @Override
    public FileContent getCloudManifest(
            int nodeID,
            Identifier objectID,
            boolean validate)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "getCloudManifest:"
                + " - nodeID=" + nodeID
                + " - objectID=" + objectID.getValue()
                + " - validate=" + validate
                );
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        FileContent state = can.getCloudManifest(objectID, validate);
        bump("getCloudManifest", startTime);
        return state;
    }
    
    @Override
    public void getCloudManifestStream(
            int nodeID,
            Identifier objectID,
            boolean validate,
            OutputStream outStream)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        if (DEBUG) System.out.println(MESSAGE + "getObjectArchiveStream called");
        can.getCloudManifestStream(
                objectID,
                validate,
                outStream);
        bump("getObjectArchive", startTime);
    }

    /**
     * Used to return a null nodeID if the StorageService is being run
     * as a CAN service
     * @param nodeID nodeID for this CAN
     * @return
     */
    protected Integer getAccessNodeID(int nodeID)
    {
        Integer localNodeID = nodeID;
        if (nodeManager.getDefaultNodeID() != null) {
            localNodeID = null;
        }
        return localNodeID;
    }
    
    @Override
    public VersionContent getVersionContent (
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        VersionContent content = can.getVersionContent(objectID, versionID);
        bump("getVersionContent", startTime);
        return content;
    }

    @Override
    public VersionState getVersionState (
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        VersionState versionState = can.getVersionState(objectID, versionID);
        URL storeURL = nodeManager.getStoreLink();
        versionState.setAccess( getAccessNodeID(nodeID), storeURL, objectID);
        bump("getVersionState", startTime);
        return versionState;
    }

    @Override
    public FileContent getFile (
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        FileContent content = can.getFile(objectID, versionID, fileName);
        bump("getFile", startTime);
        return content;
    }
    
    @Override
    public void getFileStream(
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName,
            OutputStream outStream)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        if (DEBUG) System.out.println(MESSAGE + "getObjectArchiveStream called");
        can.getFileStream(
                objectID,
                versionID,
                fileName,
                outStream);
        bump("getObjectArchive", startTime);
    }

    @Override
    public FileState getFileState (
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        if (node == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Requested node not found:" + nodeID);
        }
        NodeInf can = node.getCan();
        FileComponent fileComponent = can.getFileState(objectID, versionID, fileName);
        FileState fileState = new FileState(fileComponent);

        //System.out.println("!!!!StorageService getFileState" + fileState.dump("copy"));
        URL storeURL = nodeManager.getStoreLink();
        fileState.setAccess( getAccessNodeID(nodeID), storeURL, objectID, versionID);
        bump("getFileState", startTime);
        return fileState;
    }

    @Override
    public FileFixityState getFileFixityState (
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        if (node == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Requested node not found:" + nodeID);
        }
        NodeInf can = node.getCan();
        FileFixityState fileFixityState =  can.getFileFixityState(objectID, versionID, fileName);
        URL storeURL = nodeManager.getStoreLink();
        fileFixityState.setAccess( getAccessNodeID(nodeID), storeURL);
        bump("getFileFixityState", startTime);
        return fileFixityState;
    }

    @Override
    public ObjectFixityState getObjectFixityState (
            int nodeID,
            Identifier objectID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        if (node == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Requested node not found:" + nodeID);
        }
        NodeInf can = node.getCan();
        ObjectFixityState objectFixityState =  can.getObjectFixityState(objectID);
        objectFixityState.setPhysicalNode(nodeID);
        bump("getObjectFixityState", startTime);
        return objectFixityState;
    }

    @Override
    public FileContent getProducerVersion(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        //System.out.println(PropertiesUtil.dumpProperties("%%%%%", confProp));
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        FileContent content = can.getProducerVersion(objectID, versionID, returnIfError, archiveTypeS);
        bump("getProducerVersion", startTime);
        return content;
    }

    @Override
    public void getProducerArchiveStream(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        can.getProducerVersionStream(objectID, versionID, returnIfError, archiveTypeS, outputStream);
        bump("getVersionArchive", startTime);
    }

    @Override
    public FileContent getVersionArchive(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        FileContent content = can.getVersionArchive(objectID, versionID, returnIfError, archiveTypeS);
        bump("getVersionArchive", startTime);
        return content;
    }

    @Override
    public void getVersionArchiveStream(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        can.getVersionArchiveStream(objectID, versionID, returnIfError, archiveTypeS, outputStream);
        bump("getVersionArchive", startTime);
    }
    
    @Override
    public FileContent getVersionLink(
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        URL nodeLink = node.getNodeLink();
        if (nodeLink == null) {
            nodeLink = nodeManager.getStoreLink();
            if (DEBUG) {
                System.out.println(NAME + ".getVersionLink "
                        + " - nodeLink null - getStoreLink()=" + nodeLink);
            }
        } else {
            if (DEBUG) {
                System.out.println(NAME + ".getVersionLink "
                        + " - nodeLink=" + nodeLink);
            }
        }
        if (nodeLink == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "- getVersionLink - nodeLink missing");
        }
        Integer nodeIDD =  getAccessNodeID(nodeID);
        String nodeIDS = "";
        if (nodeIDD != null) nodeIDS = "/" + nodeIDD;
        String linkBaseURL = nodeLink.toString() + "/content" + nodeIDS;
        NodeInf can = node.getCan();
        FileContent content = can.getVersionLink(objectID, versionID, linkBaseURL);
        bump("getVersionLink", startTime);
        return content;
    }


    @Override
    public FileContent getObjectArchive(
            int nodeID,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        FileContent content = can.getObjectArchive(
                objectID,
                returnFullVersion,
                returnIfError,
                archiveTypeS);
        bump("getObjectArchive", startTime);
        return content;
    }
    
    @Override
    public void getObjectArchiveStream(
            int nodeID,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outStream)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        NodeInf can = node.getCan();
        if (DEBUG) System.out.println(MESSAGE + "getObjectArchiveStream called");
        can.getObjectStream(
                objectID,
                returnFullVersion,
                returnIfError,
                archiveTypeS,
                outStream);
        bump("getObjectArchive", startTime);
    }

    @Override
    public FileContent getObjectLink(
            int nodeID,
            Identifier objectID)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        StoreNode node = nodeManager.getStoreNode(nodeID);
        URL nodeLink = node.getNodeLink();
        if (nodeLink == null) {
            nodeLink = nodeManager.getStoreLink();
        }
        if (nodeLink == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "- getVersionLink - nodeLink missing");
        }
        String linkBaseURL = nodeLink.toString();
        linkBaseURL += "/content/" + objectID.getValue();
        NodeInf can = node.getCan();
        FileContent content = can.getObjectLink(objectID, linkBaseURL);
        bump("getObjectLink", startTime);
        return content;
    }

    @Override
    public LoggerInf getLogger(int nodeID)
    {
        StoreNode node = nodeManager.getStoreNode(nodeID);
        if (node == null) return null;
        NodeInf nodeInf = node.getCan();
        LoggerInf localLog = nodeInf.getLogger();
        return localLog;
    }
    
    protected void bump(String key, long startTime)
    {
        if (!CMDSTAT) return;
        nodeManager.bumpCmdTally(key, startTime);
        if (STATS) {
            long endTime = DateUtil.getEpochUTCDate();
            System.out.println("[TIMER-" + key + "]:" + (endTime - startTime));
        }
    }
}

