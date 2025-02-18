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
package org.cdlib.mrt.store.cloud;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.cdlib.mrt.cloud.VersionMap;



import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.Identifier;



//import org.cdlib.mrt.cloud.action.AddVersion;
import org.cdlib.mrt.cloud.action.AddVersionThread;
import org.cdlib.mrt.cloud.action.ContentCloudManifest;
import org.cdlib.mrt.cloud.action.ContentCloudManifestStream;
import org.cdlib.mrt.cloud.action.ContentFile;
import org.cdlib.mrt.cloud.action.ContentFileStream;
import org.cdlib.mrt.cloud.action.ComponentFile;
import org.cdlib.mrt.cloud.action.ContentIngestLink;
import org.cdlib.mrt.cloud.action.ContentVersionLink;
import org.cdlib.mrt.cloud.action.CopyObject;
import org.cdlib.mrt.cloud.action.DeleteCurrentVersion;
import org.cdlib.mrt.cloud.action.DeleteObject;
import org.cdlib.mrt.cloud.action.FixityFile;
import org.cdlib.mrt.cloud.action.FixityObject;
import org.cdlib.mrt.cloud.action.StateVersion;
import org.cdlib.mrt.cloud.action.StateObject;
import org.cdlib.mrt.cloud.action.UpdateVersionThread;
import org.cdlib.mrt.cloud.action.ContentVersionComponents;
import org.cdlib.mrt.cloud.utility.NormVersionMap;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.store.ObjectStoreAbs;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.store.ObjectFixityState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.store.NodeInf;
import org.cdlib.mrt.store.action.CloudArchive;
import org.cdlib.mrt.utility.StringUtil;
/**
 * @author dloy
 */
public class CANCloudService
        extends ObjectStoreAbs
{
    protected static final String NAME = "CANCloudService";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected CloudStoreInf s3service = null;
    protected String bucket = null;
    protected NormVersionMap normVersionMap = null;

    protected boolean verifyOnRead = true;
    protected boolean verifyOnWrite = true;
    
    protected String nodeIOName = null;
    
    public static CANCloudService getCANCloudService(
            NodeIO.AccessNode accessNode,
            boolean verifyOnRead,
            boolean verifyOnWrite,
            LoggerInf logger)
        throws TException
    {
        return new CANCloudService(accessNode,verifyOnRead, verifyOnWrite, logger);
    }
    
    protected CANCloudService(
            NodeIO.AccessNode accessNode,
            boolean verifyOnRead,
            boolean verifyOnWrite,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.verifyOnRead = verifyOnRead;
        this.verifyOnWrite = verifyOnWrite;
        setService(accessNode);
        
        this.bucket = bucket;
        normVersionMap = new NormVersionMap(s3service, logger);
    }

    private void setService(NodeIO.AccessNode accessNode)
        throws TException
    {
        s3service = accessNode.service;
        bucket = accessNode.container;
        String msg = "setService:"
                + " - nodeIOName=" + nodeIOName
                + " - nodeNumber=" + accessNode.nodeNumber
                + " - bucket=" + bucket
                ;
        logger.logMessage(msg, 3, true);
    }
    
    public VersionState addVersion (
                //File objectStoreBase,
                Identifier objectID,
                File manifestFile)
        throws TException
    {
        InputStream inStream = null;
        try {
            inStream = new FileInputStream(manifestFile);
            //AddVersion addVersion = AddVersion.getAddVersion(s3service, bucket, objectID, verifyOnWrite, normVersionMap, inStream, logger);        
            AddVersionThread addVersion = AddVersionThread.getAddVersionThread(s3service, bucket, objectID, verifyOnWrite, inStream, 10, logger);
            VersionState versionState = addVersion.callEx();
            
            log(MESSAGE + "addVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            return versionState;
        
        } catch (Exception ex) {
            throw makeTException(ex);
            
        } finally {
            try {
                inStream.close();
            } catch (Exception e) { }
            FileUtil.deleteTempFile(manifestFile);
        }
    }

    
    public VersionState updateVersion (
            //File storeFile,
            Identifier objectID,
            File manifestFile,
            String [] deleteList)
        throws TException
    {
        InputStream inStream = null;
        try {
            if (manifestFile != null) {
                inStream = new FileInputStream(manifestFile);
            }
            if ((deleteList != null) && (deleteList.length == 0)) deleteList = null;
            UpdateVersionThread updateVersion = UpdateVersionThread.getUpdateVersionThread(
                    s3service, 
                    bucket, 
                    objectID, 
                    verifyOnWrite,
                    inStream, 
                    deleteList,
                    10,
                    logger);
            VersionState versionState = updateVersion.callEx();
            
            log(MESSAGE + "addVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            return versionState;
        
        } catch (Exception ex) {
            ex.printStackTrace();
            throw makeTException(ex);
            
        } finally {
            try {
                inStream.close();
            } catch (Exception e) { }
            FileUtil.deleteTempFile(manifestFile);
        }
    }
    
    
    public ObjectState copyObject (
            //File storeFile,
            NodeInf fromNode,
            Identifier objectID)
        throws TException
    {
        try {
            CopyObject copyVersion = CopyObject.getCopyObject( s3service, bucket, fromNode, objectID, true, 10, logger);
            ObjectState objectState = copyVersion.callEx();
            
            log(MESSAGE + "copyObject entered"
                    + " - objectID=" + objectID
                    , 10);
            return objectState;
            
        } catch (Exception ex) {
            throw makeTException(ex);
        }
        
    }

    
    public VersionState deleteVersion (
                //File objectStoreBase,
                Identifier objectID,
                int versionID)
        throws TException
    {
        try {
            
            DeleteCurrentVersion deleteVersion = DeleteCurrentVersion.getDeleteCurrentVersion(s3service, bucket, objectID, logger);
            VersionMap map = deleteVersion.getVersionMap();
            int current = map.getCurrent();
            if (versionID == 0) versionID = current;
            if (current != versionID) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Implementation restriction: For delete version, only the current version can be deleted");
            }
            VersionState state = (VersionState)deleteVersion.callEx();
            
            log(MESSAGE + "deleteVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            return state;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    
    public ObjectState deleteObject (
                //File objectStoreBase,
                Identifier objectID)
        throws TException
    {
        try {
            
            DeleteObject deleteObject = DeleteObject.getDeleteObject(s3service, bucket, objectID, logger);
            VersionMap map = deleteObject.getVersionMap();
            int current = map.getCurrent();
            if (current == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("DELETE Object not found:" + objectID.getValue());
            }
            ObjectState state = (ObjectState)deleteObject.callEx();
            
            log(MESSAGE + "deleteObject entered"
                    + " - objectID=" + objectID
                    , 10);
            return state;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    
    public ComponentContent getVersionContent (
            //File objectStoreBase,
            Identifier objectID,
            int versionID)
        throws TException
    {
        
        try {
            ContentVersionComponents versionContent 
                    = ContentVersionComponents.getContentVersionComponents(s3service, bucket, objectID, versionID, logger);
            log(MESSAGE + "getVersionContent entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    , 10);
            ComponentContent content = versionContent.callEx();
            return content;

        } catch (Exception ex) {
            if (DEBUG) {
                ex.printStackTrace();
            }
            throw makeTException(ex);
        }
    }

    
    public ObjectState getObjectState (
            //File objectStoreBase,
            Identifier objectID)
        throws TException
    {
        try {
            StateObject stateObject = StateObject.getStateObject(s3service, bucket, objectID, logger);
            log(MESSAGE + "getObjectState entered"
                    + " - objectID=" + objectID
                    , 10);
            return (ObjectState)stateObject.callEx();

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }


    
    public VersionState getVersionState (
            //File objectStoreBase,
            Identifier objectID,
            int versionID)
        throws TException
    {
        try {
            StateVersion stateVersion = StateVersion.getStateVersion(s3service, bucket, objectID, versionID, logger);
            log(MESSAGE + "getVersionState entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    , 10);
            return (VersionState)stateVersion.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    
    public FileComponent getFileState (
            //File objectStoreBase,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        try {
            ComponentFile stateFile = ComponentFile.getComponentFile(s3service, bucket, objectID, versionID, fileID, logger);
            
            FileComponent fileState = stateFile.callEx();
            if (fileState == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        "Unable to locate file: " + fileID);
            }
            log(MESSAGE + "getFileState entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileID
                    + " - dump=" + fileState.dump("")
                    , 10);
            return fileState;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    
    public FileFixityState getFileFixityState (
            //File objectStoreBase,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        try {
            FixityFile fixityFile = FixityFile.getFixityFile(s3service, bucket, objectID, versionID, fileName, logger);
            log(MESSAGE + "FileFixityState entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileName
                    , 10);
            return (FileFixityState)fixityFile.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    
    public ObjectFixityState getObjectFixityState (
            //File objectStoreBase,
            Identifier objectID)
        throws TException
    {
        try {
            FixityObject fixityObject = FixityObject.getFixityObject(s3service, bucket, objectID,logger);
            log(MESSAGE + "FileFixityState entered"
                    + " - objectID=" + objectID
                    , 10);
            return fixityObject.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }


    
    public File getFile (
            //File objectStoreBase,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        try {
            log(MESSAGE + "getFile entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileName
                    , 10);
            ContentFile content = ContentFile.getContentFile(s3service, bucket, objectID, versionID, fileName, true, logger);
            return content.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }
    
    
    public void getFileStream (
            //File storeFile,
            Identifier objectID,
            int versionID,
            String fileName,
            OutputStream outputStream)
        throws TException
    {
        try {
            log(MESSAGE + "getFile entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileName
                    , 10);
            ContentFileStream content = ContentFileStream.getContentFileStream(
                    s3service, bucket, objectID, versionID, fileName, outputStream, logger);
            content.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    
    public FileContent getObject(
            //File objectStoreBase,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        File buildTemp = FileUtil.getTempDir("tmpbuild");
        try {
            log(MESSAGE + "getObject entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnFullVersion=" + returnFullVersion
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, buildTemp, archiveTypeS, logger)
                    .setDeleteFileAfterCopy(true);
            FileContent fileContent = cloudArchive.buildObject(archiveTypeS, returnFullVersion);
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
            
        } finally {
            try {
                buildTemp.delete();
            } catch (Exception ex) { }
        }

    }

    
    public void getObjectStream(
            //File objectStoreBase,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        File buildTemp = FileUtil.getTempDir("tmpbuild");
        try {
            log(MESSAGE + "getObject entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnFullVersion=" + returnFullVersion
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, buildTemp, archiveTypeS, logger)
                    .setDeleteFileAfterCopy(true);
            cloudArchive.buildObject(outputStream, returnFullVersion);

        } catch (Exception ex) {
            throw makeTException(ex);
            
        } finally {
            try {
                buildTemp.delete();
            } catch (Exception ex) { }
        }

    }


    
    public FileContent getCloudManifest(
            //File objectStoreBase,
            Identifier objectID,
            boolean validate)
        throws TException
    {
        try {
            log(MESSAGE + "getCloudManifest entered"
                    + " - objectID=" + objectID
                    + " - validate=" + validate
                    , 10);
            ContentCloudManifest content = ContentCloudManifest.getContentCloudManifest(s3service, bucket, objectID, validate, logger);
            FileContent fileContent = content.callEx();
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }
    
    public void getCloudManifestStream(
            //File objectStoreBase,
            Identifier objectID,
            boolean validate,
            OutputStream outStream)
        throws TException
    {
        try {
            log(MESSAGE + "getCloudManifest entered"
                    + " - objectID=" + objectID
                    + " - validate=" + validate
                    , 10);
            ContentCloudManifestStream content = ContentCloudManifestStream.getContentCloudManifestStream(s3service, bucket, objectID, validate, outStream, logger);
            content.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }

    
    public FileContent getVersionArchive(
            //File objectStoreBase,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        File buildTemp = FileUtil.getTempDir("tmpbuild");
        try {
            log(MESSAGE + "getVersionArchive entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, buildTemp, archiveTypeS, logger)
                    .setDeleteFileAfterCopy(true);
            FileContent fileContent = cloudArchive.buildVersion(versionID, archiveTypeS);
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
            
        } finally {
            try {
                buildTemp.delete();
            } catch (Exception ex) { }
        }


    }
    
    
    public void keyToFile (
            //File objectStoreBase,
            String key,
            File outFile)
        throws TException
    {
        try {
            if (key == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - key required");
            }
            CloudResponse response = new CloudResponse();
            s3service.getObject(bucket, key, outFile, response);
            Exception testEx = response.getException();
            if (testEx != null) {
                throw testEx;
            }

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    
    public void getVersionArchiveStream(
            //File objectStoreBase,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        File buildTemp = FileUtil.getTempDir("tmpbuild");
        try {
            log(MESSAGE + "getVersionArchive entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, buildTemp, archiveTypeS, logger)
                    .setDeleteFileAfterCopy(true);
            cloudArchive.buildVersion(versionID, outputStream);

        } catch (Exception ex) {
            throw makeTException(ex);
            
        } finally {
            try {
                buildTemp.delete();
            } catch (Exception ex) { }
        }


    }

    
    public FileContent getVersionLink(ContentVersionLink.Request cvlRequest)
        throws TException
     {
        try {
            log(MESSAGE + "getVersionLink entered"
                    + " - objectID=" + cvlRequest.objectID
                    + " - versionID=" + cvlRequest.versionID
                    + " - linkBaseURL=" + cvlRequest.linkBaseURL
                    , 10);
            if (logger == null) {
                System.out.println("***null logger");
            }
            cvlRequest.setS3Service(s3service)
                    . setBucket(bucket);
            ContentVersionLink versionLink = ContentVersionLink.getContentVersionLink(cvlRequest, logger);
            return versionLink.callEx();

        } catch (Exception ex) {
            ex.printStackTrace();
            throw makeTException(ex);
            
        }
     }

    public FileContent getIngestLink(
            //File objectStoreBase,
            Identifier objectID,
            int versionID,
            String linkBaseURL,
            Boolean presign,
            Boolean update)
        throws TException
    {
        try {
            log(MESSAGE + "getVersionLink entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - linkBaseURL=" + linkBaseURL
                    , 10);
            if (logger == null) {
                System.out.println("***null logger");
            }
            ContentIngestLink ingestLink = ContentIngestLink.getContentIngestLink(s3service, bucket, 
                    objectID, versionID, linkBaseURL, presign, update, logger);
            return ingestLink.callEx();

        } catch (Exception ex) {
            ex.printStackTrace();
            throw makeTException(ex);
            
        }

    }

    /**
     * get verifyOnRead switch
     *
     * @return true=option to do fixity on read, false=no fixity test should be performed
     */
    
    public boolean isVerifyOnRead() {
        return verifyOnRead;
    }

    /**
     * Set verifyOnRead switch
     * @param verifyOnReadS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    
    public void setVerifyOnRead(boolean verifyOnRead)
        throws TException
    {

        this.verifyOnRead = verifyOnRead;
    }


    /**
     * get verifyOnWrite switch
     *
     * @return true=option to do fixity on write, false=no fixity test should be performed
     */
    
    public boolean isVerifyOnWrite() {
        return verifyOnWrite;
    }

    /**
     * Set verifyOnWrite switch
     * @param verifyOnWwriteS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    
    public void setVerifyOnWrite(boolean verifyOnWrite)
        throws TException
    {
        this.verifyOnWrite = verifyOnWrite;
    }

    
    protected TException makeTException(Exception ex)
    {
        //ex.printStackTrace();
        TException tex = null;
        if (ex instanceof TException) {
            tex = (TException)ex;
            System.out.println(MESSAGE + "Run Exception: " + tex.toString());
        } else {
            tex = new TException.GENERAL_EXCEPTION(ex);
        }
        logger.logError(tex.toString(), 0);
        logger.logError(tex.dump(MESSAGE), 20);
        return tex;
    }
    
    
    public CloudStoreInf getCloudService()
    {
        return s3service;
    }
    
    public String getCloudBucket()
    {
        return bucket;
    }

    public String getNodeIOName() {
        return nodeIOName;
    }
}

