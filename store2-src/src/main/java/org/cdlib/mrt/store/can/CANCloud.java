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
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Vector;
import java.util.List;

import org.cdlib.mrt.store.NodeInf;
//import org.cdlib.mrt.store.ObjectLocationInf;
import org.cdlib.mrt.store.ObjectStoreInf;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestSAX;

import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.cloud.action.FixityObject;
import org.cdlib.mrt.store.action.ProducerComponentList;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.utility.CloudUtil;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.action.CloudArchive;
import org.cdlib.mrt.store.LastActivity;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.ObjectFixityState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.store.action.CopyNodeObject;
import org.cdlib.mrt.store.action.CopyVirtualObject;
import org.cdlib.mrt.store.KeyFileInf;
import org.cdlib.mrt.store.PreSignedState;
import org.cdlib.mrt.store.app.StorageServiceInit;
import org.cdlib.mrt.utility.ArchiveBuilder;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TLockFile;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.store.cloud.CloudObjectService;
import org.cdlib.mrt.store.cloud.CANCloudService;
import org.cdlib.mrt.store.storage.StorageServiceInf;
import org.cdlib.mrt.utility.DateUtil;

/**
 * Specific Node level service
 * @author dloy
 */
public class CANCloud
        extends CANAbs
        implements NodeInf, KeyFileInf
{
    
    public static CANCloud getCANCloud(
            LoggerInf logger,
            NodeForm nodeForm,
            CANCloudService objectCloudService,
            NodeState nodeState)
        throws TException
    {
        return new CANCloud(logger, nodeForm, objectCloudService, nodeState);
    }

    protected static final String NAME = "CAN";
    protected static final String MESSAGE = NAME + ": ";
    public static final String CANSTATS = "summary-stats.txt";
    protected SpecScheme namasteSpec = null;
    protected CANCloudService objectCloudStore;

    /**
     * CAN Constructor
     * @param logger process logging
     * @param nodeID node identifier
     * @param nodeForm
     * @param objectLocation indexer to file location (e.g. pairtree)
     * @param objectStore leaf store handling (e.g. dflat)
     * @param nodeState static information about node state
     */
    protected CANCloud(
            LoggerInf logger,
            NodeForm nodeForm,
            CANCloudService objectCloudService,
            NodeState nodeState)
        throws TException
    {
        super(logger, nodeForm, null, nodeState);
        setNodeID(nodeForm.getNodeID());
        this.objectCloudStore = objectCloudService;
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
        boolean objectAlreadyExists = false;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "addVersion - objectID required");
            }
            if ((manifestFile == null) || !manifestFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "addVersion - manifestFile missing");
            }
            versionState = objectCloudStore.addVersion(objectID, manifestFile);
            versionState.setObjectID(objectID);

            log(MESSAGE + "addVersion complete"
                + " - objectID=" + objectID
                , 10);
            return versionState;

        } catch (Exception ex) {
            System.out.println("!!!!Exception:" + ex + "Trace:" + StringUtil.stackTrace(ex));
            try {
                
            } catch (Exception tex) {

            }
            throw makeGeneralTException("addVersion", ex);
        }
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
        boolean objectAlreadyExists = false;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "updateVersion - objectID required");
            }
            if (((manifestFile == null) || !manifestFile.exists())
                    && ((deleteList == null) || (deleteList.length == 0))) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "updateVersion - either manifestFile or deleteList must be included");
            }
            
            
            versionState = objectCloudStore.updateVersion(objectID, manifestFile, deleteList);
            versionState.setObjectID(objectID);

            log(MESSAGE + "addVersion complete"
                + " - objectID=" + objectID
                , 10);
            return versionState;

        } catch (Exception ex) {
            System.out.println("!!!!Exception:" + ex + "Trace:" + StringUtil.stackTrace(ex));
            try {
                
            } catch (Exception tex) {

            }
            throw makeGeneralTException("addVersion", ex);
        }
    }


    /**
     * Delete the current version of an object
     * Note that this involves removing the content from the version
     * and properly resetting the statistics at both the dflat and can level
     * @param objectID delete current version of this object
     * @return VersionState of deleted version
     * @throws TException process exception
     */
    @Override
    public VersionState deleteVersion (
                Identifier objectID,
                int versionID)
        throws TException
    {
        VersionState versionState = null;
        boolean objectAlreadyExists = false;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "deleteVersion - objectID required");
            }
            
            versionState = objectCloudStore.deleteVersion(objectID, versionID);
            versionState.setObjectID(objectID);

            log(MESSAGE + "deleteVersion complete"
                + " - objectID=" + objectID
                , 10);
            return versionState;

        } catch (Exception ex) {
            throw makeGeneralTException("deleteVersion", ex);

        } finally {
        }
    }

    /**
     * Delete object with all contained version
     * Note that this involves removing the content from the version
     * and properly resetting the statistics at both the dflat and can level
     * @param objectID delete this object
     * @return VersionState of deleted version
     * @throws TException process exception
     */
    @Override
    public ObjectState deleteObject (
                Identifier objectID)
        throws TException
    {
        ObjectState objectState = null;
        boolean objectAlreadyExists = false;
        File parentObject = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "deleteObject - objectID required");
            }
            objectState = objectCloudStore.deleteObject(objectID);
            log(MESSAGE + "deleteObject complete"
                + " - objectID=" + objectID
                , 10);
            return objectState;
            
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw makeGeneralTException("deleteObject", ex);

        } finally {
            FileUtil.deleteEmptyPath(parentObject);
        }
    }

    @Override
    public FileContent getVersionLink(
            Identifier objectID,
            int versionID,
            String linkBaseURL,
            Boolean presign)
        throws TException
    {
        FileContent manifestFile = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - objectID required");
            }
            if (StringUtil.isEmpty(linkBaseURL)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - linkBaseURL not supplied");
            }

            manifestFile = objectCloudStore.getVersionLink(objectID, versionID, linkBaseURL, presign);

            log(MESSAGE + "getVersionLink entered"
                + " - objectID=" + objectID
                + " - versionID=" + versionID
                + " - baseURL=" + linkBaseURL
                , 10);
            return manifestFile;

        } catch (Exception ex) {
            throw makeGeneralTException("getVersionLink", ex);
        }
    }

    @Override
    public VersionState getVersionState (
                Identifier objectID,
                int versionID)
        throws TException
    {
        VersionState versionState = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionState - objectID required");
            }

            versionState = objectCloudStore.getVersionState(objectID, versionID);
            versionState.setObjectID(objectID);
            log(MESSAGE + "getVersionState entered"
                + " - objectID=" + objectID
                + " - versionID=" + versionID
                , 10);
            return versionState;

        } catch (Exception ex) {
            throw makeGeneralTException("getVersionState", ex);
        }
    }

    @Override
    public ObjectState getObjectState (
                Identifier objectID)
        throws TException
    {
        ObjectState objectState = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getObjectState - objectID required");
            }
            

            objectState = objectCloudStore.getObjectState(objectID);
            log(MESSAGE + "getObjectState entered"
                + " - objectID=" + objectID
                , 10);
            return objectState;

        } catch (Exception ex) {
            throw makeGeneralTException("getObjectState", ex);
        }
    }

    @Override
    public VersionContent getVersionContent (
                Identifier objectID,
                int versionID)
        throws TException
    {
        VersionContent versionContent = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionState - objectID required");
            }

            ComponentContent componentContent = objectCloudStore.getVersionContent(objectID, versionID);
            versionContent = new VersionContent(componentContent);
            log(MESSAGE + "getVersionState entered"
                + " - objectID=" + objectID
                + " - versionID=" + versionID
                , 10);
            return versionContent;


        } catch (Exception ex) {
            throw makeGeneralTException("getVersionContent", ex);
        }
    }

    @Override
    public FileComponent getFileState (
                Identifier objectID,
                int versionID,
                String fileName)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFileState - objectID required");
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFileState - objectID required");
            }
            return objectCloudStore.getFileState(objectID, versionID, fileName);

        } catch (Exception ex) {
            throw makeGeneralTException("getFileState", ex);
        }
    }

    @Override
    public FileContent getFile (
                Identifier objectID,
                int versionID,
                String fileName)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - objectID required");
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - fileName required for " + objectID.getValue());
            }
            File returnFile = objectCloudStore.getFile(objectID, versionID, fileName);
            File tempFile = returnFile;
            if (!isTempCldFile(returnFile)) {
                tempFile = FileUtil.copy2Temp(returnFile);
            }
            FileComponent fileState = objectCloudStore.getFileState(objectID, versionID, fileName);
            FileContent fileContent = FileContent.getFileContent(fileState, null, tempFile);
            return fileContent;

        } catch (Exception ex) {
            throw makeGeneralTException("getFile", ex);
        }
    }
    

    //@Override
    public void keyToFile (
                String key, File outFile)
        throws TException
    {
        try {
            if (key == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - key required");
            }

            String [] parts = key.split("\\|");
            if (parts.length < 3) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "keyToFile - outFile required");
            }
            Identifier objectID = new Identifier(parts[0]);
            
            
            objectCloudStore.keyToFile(key, outFile);
            
        } catch (Exception ex) {
            throw makeGeneralTException("getFile", ex);
        }
    }

    @Override
    public void getFileStream (
                Identifier objectID,
                int versionID,
                String fileName,
                OutputStream outStream)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - objectID required");
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - fileName required for " + objectID.getValue());
            }
            objectCloudStore.getFileStream(objectID, versionID, fileName, outStream);

        } catch (Exception ex) {
            throw makeGeneralTException("getFile", ex);
        }
    }


    @Override
    public FileFixityState getFileFixityState (
                Identifier objectID,
                int versionID,
                String fileName)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFileState - objectID required");
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFileState - objectID required");
            }
            FileFixityState fileFixityState = objectCloudStore.getFileFixityState(
                    objectID, versionID, fileName);
            fileFixityState.setObjectID(objectID);
            fileFixityState.setVersionID(versionID);
            fileFixityState.setFileName(fileName);
            return fileFixityState;

        } catch (Exception ex) {
            throw makeGeneralTException("getFileFixityState", ex);
        }
    }

    @Override
    public ObjectFixityState getObjectFixityState (
                Identifier objectID)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFileState - objectID required");
            }
            
            ObjectFixityState objectFixityState = objectCloudStore.getObjectFixityState(objectID);
            return objectFixityState;

        } catch (Exception ex) {
            throw makeGeneralTException("getFileFixityState", ex);
        }
    }

    @Override
    public NodeState getNodeState()
        throws TException
    {
        if (nodeForm == null) {
            throw new TException.INVALID_OR_MISSING_PARM (
                    MESSAGE + "getNodeState - missing nodeForm");
        }
        NodeState currentNodeState = nodeForm.getNodeState();
        if ((currentNodeState.getTestOk() == null) || currentNodeState.getTestOk()) {
            if (DEBUG) System.out.println("***Peforming nodeStateTest:" + currentNodeState.getIdentifier());
            String bucket = objectCloudStore.getCloudBucket();
            CloudStoreInf cloudService = objectCloudStore.getCloudService();
            StateHandler.RetState returnCloudState = null;
            try {
                returnCloudState = cloudService.getState(bucket);
            } catch(Exception ex) {
                returnCloudState = new StateHandler.RetState(bucket, null, ex.toString());
            }
            if (returnCloudState != null) {
                currentNodeState.setOk(returnCloudState.getOk());
                currentNodeState.setError(returnCloudState.getError());
            }
        }
        
        return currentNodeState;
    }

    @Override
    public FileContent getObjectArchive(
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "objectID required");
            }
            FileContent objectArchive = objectCloudStore.getObject(
                    objectID,
                    returnFullVersion,
                    returnIfError,
                    archiveTypeS);
            return objectArchive;

        } catch (Exception ex) {
            throw makeGeneralTException("getObjectArchive", ex);
            
        } 
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
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "objectID required");
            }
            objectCloudStore.getObjectStream(
                    objectID,
                    returnFullVersion,
                    returnIfError,
                    archiveTypeS,
                    outputStream);
            return;

        } catch (Exception ex) {
            throw makeGeneralTException("getObjectArchive", ex);
            
        } 
    }

    @Override
    public FileContent getCloudManifest(
            Identifier objectID,
            boolean validate)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "objectID required");
            }
            
            FileContent objectArchive = objectCloudStore.getCloudManifest(
                    objectID, validate);
            return objectArchive;

        } catch (Exception ex) {
            throw makeGeneralTException("getObjectArchive", ex);
            
        } 
    }

    @Override
    public void getCloudManifestStream(
            Identifier objectID,
            boolean validate,
            OutputStream outStream)
        throws TException
    {  
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "objectID required");
            }
            objectCloudStore.getCloudManifestStream(
                    objectID, validate, outStream);

        } catch (Exception ex) {
            throw makeGeneralTException("getObjectArchive", ex);
            
        }
    }

    @Override
    public FileContent getVersionArchive(
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - objectID required");
            }
            FileContent versionArchive = objectCloudStore.getVersionArchive(
                    objectID, versionID, returnIfError, archiveTypeS);
            return versionArchive;

        } catch (Exception ex) {
            throw makeGeneralTException("getVersionArchive", ex);
            
        }
    }
    
    @Override
    public FileContent getProducerVersion(
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - objectID required");
            }
            
            
            FileContent objectManifest = objectCloudStore.getCloudManifest(
                    objectID, false);
            VersionMap versionMap = getVersionMap(objectManifest.getFile());
            
            int nameVer = versionID;
            if (nameVer == 0) nameVer = versionMap.getCurrent();
            String archiveName = getArchiveName(objectID, nameVer);
            File baseDir = FileUtil.getTempDir(archiveName);
            CloudStoreInf s3service = objectCloudStore.getCloudService();
            String bucket = objectCloudStore.getCloudBucket();
            List<String> producerFilter = nodeState.retrieveProducerFilter();
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, baseDir, archiveTypeS, logger);
            FileContent producerArchive = cloudArchive.buildProducer(versionID, producerFilter, archiveName);
            return producerArchive;
            

         } catch (Exception ex) {
            System.out.println("getProducerVersion Exception:" + ex);
            ex.printStackTrace();
            throw makeGeneralTException("getProducerVersion", ex);
         }
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
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - objectID required");
            }
            FileContent objectManifest = objectCloudStore.getCloudManifest(
                    objectID, false);
            VersionMap versionMap = getVersionMap(objectManifest.getFile());
            
            int nameVer = versionID;
            if (nameVer == 0) nameVer = versionMap.getCurrent();
            String archiveName = getArchiveName(objectID, nameVer);
            File baseDir = FileUtil.getTempDir(archiveName);
            CloudStoreInf s3service = objectCloudStore.getCloudService();
            String bucket = objectCloudStore.getCloudBucket();
            List<String> producerFilter = nodeState.retrieveProducerFilter();
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, baseDir, archiveTypeS, logger);
            cloudArchive.buildProducer(versionID, producerFilter, outputStream);

         } catch (Exception ex) {
            throw makeGeneralTException("getVersionArchiveStream", ex);
         }
    }

    public static String getArchiveName(
            Identifier objectID,
            int versionID)
    {
        String object = objectID.getValue();
        if (object.indexOf("ark:/") == 0) {
            object = "ark-" + object.substring(5);
        }
        object = object.replace("/", "-");
        return object + "#" + versionID;
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
         
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - objectID required");
            }
            objectCloudStore.getVersionArchiveStream(
                    objectID, versionID, returnIfError, archiveTypeS, outputStream);

        } catch (Exception ex) {
            throw makeGeneralTException("getVersionArchiveStream", ex);
            
        } 
    }

    protected TException makeGeneralTException(String header, Exception ex)
    {
        ex.printStackTrace();
        TException tex = null;
        if (ex instanceof TException) {
            tex = (TException)ex;
        } else {
            tex = new TException.GENERAL_EXCEPTION(ex);
        }
        logger.logError(tex.toString(), 0);
        logger.logError(tex.dump(header), 10);
        return tex;
    }

    public SpecScheme getNamasteSpec()
    {
        return namasteSpec;
    }

    protected void buildNamasteFile(File namasteFile)
        throws TException
    {
        this.namasteSpec.buildNamasteFile(namasteFile);
    }

    protected void setLocalIDDb()
        throws TException
    {

    }

    public static List<String> getLocalIDs(String vals)
    {
        Vector<String> idList = new Vector<String>(10);
        if (StringUtil.isEmpty(vals)) return null;
        String [] ids = vals.split("\\s*\\;\\s*");
        for (String id : ids) {
            id = id.trim();
            idList.add(id);
        }
        return idList;
    }
    
    @Override
    public ObjectState copyObject (
            Identifier objectID)
        throws TException
    {
        throw new TException.REQUEST_INVALID("copyObject not allowed on physical CAN only on virtual");
    }

    @Override
    public ObjectState copyObject(String storageBaseURI, Identifier objectID, NodeInf targetNode)
        throws TException
    {
        try {
            
            CopyNodeObject co = CopyNodeObject.getCopyNodeObject(storageBaseURI, this, targetNode, objectID, logger);
            ObjectState state = co.callEx();
            if (state == null) {
                System.out.println("***State null");
            }
            return state;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void backout(Identifier objectID, int fromVersionID, int toVersionID)
        throws TException
    {
        try {
            if (toVersionID == 0) return;
            if (fromVersionID == 1) { // delete object
                try {
                    ObjectState objectState = deleteObject(objectID);
                    return;
                } catch (Exception ex) {
                    System.out.println("Unable to delete object:"
                        + " - objectID=" + objectID.getValue()
                        + " - Exception=" + ex.toString()
                        );
                }
                
            }
            for (int currentVersion=toVersionID; currentVersion >= 1; currentVersion--) {
                try {
                    VersionState toVersion = deleteVersion(objectID, currentVersion);
                } catch (Exception ex) {
                    System.out.println("Unable to back out version:"
                        + " - objectID=" + objectID.getValue()
                        + " - Exception=" + ex.toString()
                        );
                }
            }
        } catch (Exception ex) {
            
        }
    }

    protected static String manifest2BaseContent(URL manifestURL)
        throws TException
    {
        try {
            String manifestURLS = manifestURL.toString();
            int pos = manifestURLS.indexOf("/manifest/");
            if (pos < 0) {
                throw new TException.REQUEST_INVALID("Invalid manifest URL:" + manifestURL);
            }
            String [] seg = manifestURLS.split("/");
            StringBuffer buf = new StringBuffer();
            for (int i=0; i<seg.length; i++) {
                String ele = seg[i];
                if (buf.length() > 0) buf.append("/");
                if (ele.equals("manifest")) {
                    if (i == (seg.length - 1)) { // last seg
                        throw new TException.REQUEST_INVALID("Invalid manifest URL 2:" + manifestURL);
                    }
                    buf.append("content");
                    buf.append("/" + seg[i+1]);
                    break;
                } else {
                    buf.append(ele);
                }
            }
            return buf.toString();
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
}