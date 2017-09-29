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
package org.cdlib.mrt.store.can;

import org.cdlib.mrt.store.NodeInf;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.cdlib.mrt.cloud.ManifestSAX;



import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.store.action.CopyVirtualObject;
import org.cdlib.mrt.store.action.CopyNodeObject;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;


/**
 *
 * @author dloy
 */
public class CANVirtual
        extends CANAbs
        implements NodeInf

{
    public enum CanStatus {source_only, target_only, source_and_target, invalid_migration, none};
    protected static final String NAME = "CANVirtual";
    protected static final String MESSAGE = NAME + ": ";

    protected boolean debug = false;
    protected NodeInf sourceNode = null;
    protected NodeInf targetNode = null;
    protected String storageBaseURI = null;


    /**
     * Constructor - remote can
     * @param remoteCANURL - URL to remote can
     * @param logger - local logger to be used with this can
     * @throws TException
     */
    public static CANVirtual getCANVirtual(
            NodeState nodeState,
            NodeInf sourceNode,
            NodeInf targetNode,
            String storageBase,
            LoggerInf logger)
        throws TException
    {
        return new CANVirtual(nodeState, sourceNode, targetNode, storageBase, logger);
    }

    /**
     * Constructor - remote can
     * @param remoteCANURL - URL to remote can
     * @param logger - local logger to be used with this can
     * @throws TException
     */
    protected CANVirtual(
            NodeState nodeState,
            NodeInf sourceNode,
            NodeInf targetNode,
            String storageBaseURI,
            LoggerInf logger)
        throws TException
    {
        super(null, logger);
        this.nodeState = nodeState;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.storageBaseURI = storageBaseURI;
        nodeID = nodeState.getIdentifier();
    }

    @Override
    public VersionState addVersion (
            Identifier objectID,
            String context,
            String localID,
            File manifestFile)
        throws TException
    {
        VersionState versionState = null;
        CanStatus status = getCanStatusFix(objectID);
        if (status == CanStatus.source_only) {
            copyObject(objectID);
        }
        
        versionState = targetNode.addVersion(objectID, context, localID, manifestFile);
        versionState.setPhysicalNode(targetNode.getNodeID());
        return versionState;
    }

    @Override
    public VersionState updateVersion (
            Identifier objectID,
            String context,
            String localID,
            File manifestFile,
            String [] deleteList)
        throws TException
    {
        VersionState versionState = null;
        CanStatus status = getCanStatusFix(objectID);
        if (status == CanStatus.source_only) {
            copyObject(objectID);
        }
        
        versionState =  targetNode.updateVersion(objectID, context, localID, manifestFile, deleteList);
        versionState.setPhysicalNode(targetNode.getNodeID());
        return versionState;
    }
    
    @Override
    public VersionState deleteVersion (
            Identifier objectID,
            int versionID)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE("Virtual node does not support delete:"
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID
                );
    }

    @Override
    public ObjectState deleteObject (
            Identifier objectID)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE("Virtual node does not support delete:"
                + " - objectID=" + objectID.getValue() 
                );
    }


    @Override
    public ObjectState getObjectState (
            Identifier objectID)
        throws TException
    {
        NodeInf physicalNode = null;
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            physicalNode = targetNode;
            
        } else if (status == CanStatus.source_only) {
            physicalNode = sourceNode;
            
        } else {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("object identifier not found:" + objectID.getValue());
        }
        
        ObjectState objectState = physicalNode.getObjectState(objectID);
        objectState.setPhysicalNode(physicalNode.getNodeID());
        return objectState;
    }

    @Override
    public VersionContent getVersionContent (
            Identifier objectID,
            int versionID)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getVersionContent(objectID, versionID);
        }
        if (status == CanStatus.source_only) {
            return sourceNode.getVersionContent(objectID, versionID);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("Version Content: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID
                );
    }

    @Override
    public VersionState getVersionState (
            Identifier objectID,
            int versionID)
        throws TException
    {
        NodeInf physicalNode = null;
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            physicalNode = targetNode;
            
        } else if (status == CanStatus.source_only) {
            physicalNode = sourceNode;
            
        } else {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Version State: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID
                );
        }
        
        VersionState versionState = physicalNode.getVersionState(objectID, versionID);
        versionState.setPhysicalNode(physicalNode.getNodeID());
        return versionState;
        
    }

    @Override
    public FileContent getFile (
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getFile(objectID, versionID, fileName);
        }
        if (status == CanStatus.source_only) {
            return sourceNode.getFile(objectID, versionID, fileName);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getFile: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID 
                + " - fileName=" + fileName
                );
    }

    @Override
    public void getFileStream (
            Identifier objectID,
            int versionID,
            String fileName,
            OutputStream outputStream)
        throws TException
    {
     
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            targetNode.getFileStream(objectID, versionID, fileName, outputStream);
            return;
        }
        if (status == CanStatus.source_only) {
            sourceNode.getFileStream(objectID, versionID, fileName, outputStream);
            return;
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getFileStream: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID 
                + " - fileName=" + fileName
                );
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
    @Override
    public FileComponent getFileState (
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        NodeInf physicalNode = null;
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            physicalNode = targetNode;
            
        } else if (status == CanStatus.source_only) {
            physicalNode = sourceNode;
            
        } else {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("getFileState: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID
                + " - fileName=" + fileName
                );
        }
        
        return physicalNode.getFileState(objectID, versionID, fileName);
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
    @Override
    public FileFixityState getFileFixityState (
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
 
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getFileFixityState(objectID, versionID, fileName);
        }
        if (status == CanStatus.source_only) {
            return sourceNode.getFileFixityState(objectID, versionID, fileName);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getFileFixityState: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID 
                + " - fileName=" + fileName
                );
    }

    @Override
    public NodeState getNodeState()
        throws TException
    {
        return nodeState;
    }
    
    @Override
    public FileContent getProducerVersion(
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
            throw new TException.UNIMPLEMENTED_CODE("Not supported");
    }
    
 @Override
    public void getProducerVersionStream(
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
            throw new TException.UNIMPLEMENTED_CODE("Not supported");
    }
    
    @Override
    public FileContent getVersionArchive(
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);

        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getVersionArchive(objectID, versionID, returnIfError, archiveTypeS);
        }
        if (status == CanStatus.source_only) {
            return getVersionArchive(objectID, versionID, returnIfError, archiveTypeS);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getVersionArchive: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID 
                + " - returnIfError=" + returnIfError
                + " - archiveTypeS=" + archiveTypeS
                );
    }

    @Override
    public void getVersionArchiveStream(
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);

        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            targetNode.getVersionArchiveStream(objectID, versionID, returnIfError, archiveTypeS, outputStream);
            return;
        }
        if (status == CanStatus.source_only) {
            sourceNode.getVersionArchiveStream(objectID, versionID, returnIfError, archiveTypeS, outputStream);
            return;
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getVersionArchiveStream: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID 
                + " - returnIfError=" + returnIfError
                + " - archiveTypeS=" + archiveTypeS
                );
    }


    @Override
    public FileContent getObjectArchive(
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getObjectArchive(objectID, returnFullVersion, returnIfError, archiveTypeS);
        }
        if (status == CanStatus.source_only) {
            return sourceNode.getObjectArchive(objectID, returnFullVersion, returnIfError, archiveTypeS);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getObjectArchive: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - returnFullVersion=" + returnFullVersion 
                + " - returnIfError=" + returnIfError
                + " - archiveTypeS=" + archiveTypeS
                );
    }

    @Override
    public void getObjectStream(
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {

        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            targetNode.getObjectStream(objectID, returnFullVersion,  returnIfError, archiveTypeS, outputStream);
            return;
        }
        if (status == CanStatus.source_only) {
            sourceNode.getObjectStream(objectID, returnFullVersion, returnIfError, archiveTypeS, outputStream);
            return;
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getObjectStream: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - returnFullVersion=" + returnFullVersion 
                + " - returnIfError=" + returnIfError
                + " - archiveTypeS=" + archiveTypeS
                );
    }

    //"manifest/{nodeid}/{objectid}"
    @Override
    public FileContent getCloudManifest(
            Identifier objectID,
            boolean validate)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getCloudManifest(objectID, validate);
        }
        if (status == CanStatus.source_only) {
            return sourceNode.getCloudManifest(objectID, validate);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getCloudManifest: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - validate=" + validate 
                );
    }
    
    @Override
    public void getCloudManifestStream(
            Identifier objectID,
            boolean validate,
            OutputStream outputStream)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            targetNode.getCloudManifestStream(objectID, validate, outputStream);
            return;
        }
        if (status == CanStatus.source_only) {
            sourceNode.getCloudManifestStream(objectID, validate, outputStream);
            return;
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getCloudManifestStream: object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - validate=" + validate 
                );
    }

    @Override
    public ObjectState copyObject(Identifier objectID)
        throws TException
    {
        try {
            CopyVirtualObject cvo = CopyVirtualObject.getCopyVirtualObject(storageBaseURI, sourceNode, targetNode, objectID, logger);
            return cvo.callEx();
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    @Override
    public ObjectState copyObject(String storageBaseURIIgnore, Identifier objectID, NodeInf targetNode)
        throws TException
    {
        try {
            CopyNodeObject cvo = CopyNodeObject.getCopyNodeObject(storageBaseURI, sourceNode, targetNode, objectID, logger);
            return cvo.callEx();
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public VersionMap getVersionMap(Identifier objectID)
        throws TException
    {
        return null;
    }

    
    @Override
    public FileContent getVersionLink(
            Identifier objectID,
            int versionID,
            String linkBaseURL)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getVersionLink(objectID, versionID, linkBaseURL);
        }
        if (status == CanStatus.source_only) {
            return sourceNode.getVersionLink(objectID, versionID, linkBaseURL);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getVersionLink object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - versionID=" + versionID
                + " - linkBaseURL=" + linkBaseURL
                );
    }

    @Override
    public FileContent getObjectLink(
            Identifier objectID,
            String linkBaseURL)
        throws TException
    {
        CanStatus status = getCanStatus(objectID);
        if ((status == CanStatus.source_and_target) || (status == CanStatus.target_only)) {
            return targetNode.getObjectLink(objectID, linkBaseURL);
        }
        if (status == CanStatus.source_only) {
            return sourceNode.getObjectLink(objectID, linkBaseURL);
        }
        throw new TException.REQUESTED_ITEM_NOT_FOUND("getObjectLink object not found" 
                + " - objectID=" + objectID.getValue() 
                + " - linkBaseURL=" + linkBaseURL
                );
    }
    
    protected CanStatus getCanStatus(Identifier objectID)
        throws TException
    {
        CanStatus status = getCanStatusValidate(objectID);
        if (status == CanStatus.invalid_migration) {
            status = CanStatus.source_only;
        }
        return status;
    }
    
    protected CanStatus getCanStatusException(Identifier objectID)
        throws TException
    {
        CanStatus status = getCanStatusValidate(objectID);
        if (status == CanStatus.invalid_migration) {
            String msg =
                        "Virtual copy error - Source and Target Node mismatch content on virtual node\n"
                        + " - objectID:" + objectID.getValue()
                        + " - source node:" + sourceNode.getNodeID()
                        + " - target node:" + targetNode.getNodeID();
            throw new TException.INVALID_DATA_FORMAT(MESSAGE + msg);
        }
        return status;
    }
    
    protected CanStatus getCanStatusFix(Identifier objectID)
        throws TException
    {
        CanStatus status = getCanStatusValidate(objectID);
        if (status == CanStatus.invalid_migration) {
            deleteTarget(objectID);
            status = CanStatus.source_only;
        }
        return status;
    }
    
    protected CanStatus getCanStatusValidate(Identifier objectID)
        throws TException
    {
        boolean isTarget = false;
        boolean isSource = false;
        CanStatus status = null;
        ObjectState sourceObject = null;
        ObjectState targetObject = null;
        
        try {
            sourceObject = sourceNode.getObjectState(objectID);
            isSource = true;
        } catch (Exception ex) { }
        try {
            targetObject = targetNode.getObjectState(objectID);
            isTarget = true;
        } catch (Exception ex) { }
        if (isTarget) {
            if (isSource) status = CanStatus.source_and_target;
            else status = CanStatus.target_only;
            
        } else {
            if (isSource) status = CanStatus.source_only;
            else status = CanStatus.none;
        }
        
        if (status  == CanStatus.source_and_target) {
            VersionMap sourceMap = getNodeMap(sourceNode, objectID);
            VersionMap targetMap = getNodeMap(targetNode, objectID);
            int sourceCurrent = sourceMap.getCurrent();
            int targetCurrent = targetMap.getCurrent();
        
            if (sourceCurrent >= targetCurrent) {
                String msg =
                        "Source and Target Node exist for virtual node"
                        + " and Source version count > Target version count\n"
                        + " - objectID:" + objectID.getValue()
                        + " - source node:" + sourceNode.getNodeID()
                        + " - source current:" + sourceCurrent + "\n"
                        + " - target node:" + targetNode.getNodeID()
                        + " - target current:" + targetCurrent;
                System.out.println(MESSAGE + "WARNING - " + msg);
                status = CanStatus.invalid_migration;
                return status;
            }
            
            for (int version = 1; version <= sourceCurrent; version++) {
                VersionMap.VersionStats sourceStats = sourceMap.getVersionStats(version);
                VersionMap.VersionStats targetStats = targetMap.getVersionStats(version);
                if (sourceStats.totalSize != targetStats.totalSize) {
                    String msg =
                        "Source and Target Node exist for virtual node"
                        + " and version sizes do not match\n"
                        + " - objectID:" + objectID.getValue()
                        + " - version:" + version + "\n"
                        + " - source node:" + sourceNode.getNodeID()
                        + " - source size:" + sourceStats.totalSize + "\n"
                        + " - target node:" + targetNode.getNodeID()
                        + " - target size:" + targetStats.totalSize;
                    System.out.println(MESSAGE + "WARNING - " + msg);
                    status = CanStatus.invalid_migration;
                    return status;
                }
           }
        }
        return status;
    }
    
    protected void deleteTarget(Identifier objectID)
        throws TException
    {
        String msg = MESSAGE + "Delete invalid target - "
                    + objectID.getValue() + " deleted on node:" + targetNode.getNodeID();
        try {
            ObjectState targetDeleteState = targetNode.deleteObject(objectID);
            System.out.println("Delete OK:" + msg
            );
        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE("Delete Fails:" + msg + "\n" + ex);
        }
    }
    
    protected VersionMap getNodeMap(NodeInf processNode, Identifier objectID)
        throws TException
    {
        try {
            FileContent processFile = processNode.getCloudManifest(objectID, false);
            InputStream inStream = new FileInputStream(processFile.getFile());
            return ManifestSAX.buildMap(inStream, logger);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }
}
