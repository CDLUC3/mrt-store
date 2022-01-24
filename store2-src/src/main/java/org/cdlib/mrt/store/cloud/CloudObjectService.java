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
//import org.cdlib.mrt.cloud.action.ContentObjectLink;
//import org.cdlib.mrt.cloud.action.ContentVersionLink;
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
import org.cdlib.mrt.store.ObjectStoreInf;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.store.ObjectFixityState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.cloud.action.ContentVersionLink;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.NodeInf;
import org.cdlib.mrt.store.action.CloudArchive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dloy
 */
public class CloudObjectService
        extends ObjectStoreAbs
        implements ObjectStoreInf
{
    protected static final String NAME = "CloudObjectService";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected Logger ecslogger = LoggerFactory.getLogger("ecslogger.store.cloud.CloudObjectService");

    protected CloudStoreInf s3service = null;
    protected String bucket = null;
    protected NormVersionMap normVersionMap = null;

    protected boolean verifyOnRead = true;
    protected boolean verifyOnWrite = true;
    
    public static CloudObjectService getCloudObjectState(
            CloudStoreInf s3service,
            String bucket,
            LoggerInf logger)
        throws TException
    {
        return new CloudObjectService(s3service, bucket, logger);
    }
    
    protected CloudObjectService(
            CloudStoreInf s3service,
            String bucket,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.s3service = s3service;
        this.bucket = bucket;
        normVersionMap = new NormVersionMap(s3service, logger);
    }

    @Override
    public VersionState addVersion (
                File objectStoreBase,
                Identifier objectID,
                File manifestFile)
        throws TException
    {
        try {
            InputStream inStream = new FileInputStream(manifestFile);
            //AddVersion addVersion = AddVersion.getAddVersion(s3service, bucket, objectID, verifyOnWrite, normVersionMap, inStream, logger);        
            AddVersionThread addVersion = AddVersionThread.getAddVersionThread(s3service, bucket, objectID, verifyOnWrite, inStream, 10, logger);
            VersionState versionState = addVersion.callEx();
            
            log(MESSAGE + "addVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            ecslogger.debug("{}: addVersion entered - objectID={}", NAME, objectID);
            return versionState;
        
        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    @Override
    public VersionState updateVersion (
            File storeFile,
            Identifier objectID,
            File manifestFile,
            String [] deleteList)
        throws TException
    {
        try {
            InputStream inStream = null;
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
            
            log(MESSAGE + "updateVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            ecslogger.debug("{}: updateVersion entered - objectID={}", NAME, objectID);
            return versionState;
        
        } catch (Exception ex) {
            ex.printStackTrace();
            throw makeTException(ex);
        }
    }
    
    @Override
    public ObjectState copyObject (
            File storeFile,
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
            ecslogger.debug("{}: copyObject entered - objectID={}", NAME, objectID);
            return objectState;
            
        } catch (Exception ex) {
            throw makeTException(ex);
        }
        
    }

    @Override
    public VersionState deleteVersion (
                File objectStoreBase,
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
            ecslogger.debug("{}: deleteVersion entered - objectID={}", NAME, objectID);
            return state;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    @Override
    public ObjectState deleteObject (
                File objectStoreBase,
                Identifier objectID)
        throws TException
    {
        try {
            
            DeleteObject deleteObject = DeleteObject.getDeleteObject(s3service, bucket, objectID, logger);
            VersionMap map = deleteObject.getVersionMap();
            int current = map.getCurrent();
            if (current == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Object not found:" + objectID.getValue());
            }
            ObjectState state = (ObjectState)deleteObject.callEx();
            
            log(MESSAGE + "deleteObject entered"
                    + " - objectID=" + objectID
                    , 10);
            ecslogger.debug("{}: deleteObject entered - objectID={}", NAME, objectID);
            return state;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    @Override
    public ComponentContent getVersionContent (
            File objectStoreBase,
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
            ecslogger.debug("{}: getVersionContent entered - objectID={} - versionID={}", NAME, objectID, versionID);
            ComponentContent content = versionContent.callEx();
            return content;

        } catch (Exception ex) {
            if (DEBUG) {
                ex.printStackTrace();
            }
            throw makeTException(ex);
        }
    }

    @Override
    public ObjectState getObjectState (
            File objectStoreBase,
            Identifier objectID)
        throws TException
    {
        try {
            StateObject stateObject = StateObject.getStateObject(s3service, bucket, objectID, logger);
            log(MESSAGE + "getObjectState entered"
                    + " - objectID=" + objectID
                    , 10);
            ecslogger.debug("{}: getObjectState entered - objectID={}", NAME, objectID);
            return (ObjectState)stateObject.callEx();

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }


    @Override
    public VersionState getVersionState (
            File objectStoreBase,
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
            ecslogger.debug("{}: getVersionState entered - objectID={} - versionID={}", NAME, objectID, versionID);
            return (VersionState)stateVersion.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    @Override
    public FileComponent getFileState (
            File objectStoreBase,
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
            ecslogger.debug("{}: getFileState entered - objectID={} - versionID={} - fileName={} - dump={}", NAME, objectID, versionID, fileID, fileState.dump(""));
            return fileState;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    @Override
    public FileFixityState getFileFixityState (
            File objectStoreBase,
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
            ecslogger.debug("{}: getFileFixityState entered - objectID={} - versionID={} - fileName={}", NAME, objectID, versionID, fileName);
            return (FileFixityState)fixityFile.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    @Override
    public ObjectFixityState getObjectFixityState (
            File objectStoreBase,
            Identifier objectID)
        throws TException
    {
        try {
            FixityObject fixityObject = FixityObject.getFixityObject(s3service, bucket, objectID,logger);
            log(MESSAGE + "FileFixityState entered"
                    + " - objectID=" + objectID
                    , 10);
            ecslogger.debug("{}: getObjectFixityState entered - objectID={}", NAME, objectID);
            return fixityObject.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }


    @Override
    public File getFile (
            File objectStoreBase,
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
            ecslogger.debug("{}: getFile entered - objectID={} - versionID={} - fileName={}", NAME, objectID, versionID, fileName);
            ContentFile content = ContentFile.getContentFile(s3service, bucket, objectID, versionID, fileName, true, logger);
            return content.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }
    
    @Override
    public void getFileStream (
            File storeFile,
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
            ecslogger.debug("{}: getFileStream entered - objectID={} - versionID={} - fileName={}", NAME, objectID, versionID, fileName);
            ContentFileStream content = ContentFileStream.getContentFileStream(
                    s3service, bucket, objectID, versionID, fileName, outputStream, logger);
            content.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    @Override
    public FileContent getObject(
            File objectStoreBase,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        try {
            log(MESSAGE + "getObject entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnFullVersion=" + returnFullVersion
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            ecslogger.debug("{}: getObject entered - objectID={} - archiveTypeS={} - returnFullVersion={} - returnIfError={} - archiveTypeS={}",
                    NAME, objectID, archiveTypeS, returnFullVersion, returnIfError, archiveTypeS);
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, objectStoreBase, archiveTypeS, logger);
            FileContent fileContent = cloudArchive.buildObject(archiveTypeS, returnFullVersion);
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }

    @Override
    public void getObjectStream(
            File objectStoreBase,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        try {
            log(MESSAGE + "getObject entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnFullVersion=" + returnFullVersion
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            ecslogger.debug("{}: getObjectStream entered - objectID={} - archiveTypeS={} - returnFullVersion={} - returnIfError={} - archiveTypeS={}",
                    NAME, objectID, archiveTypeS, returnFullVersion, returnIfError, archiveTypeS);
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, objectStoreBase, archiveTypeS, logger);
            cloudArchive.buildObject(outputStream, returnFullVersion);

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }


    @Override
    public FileContent getCloudManifest(
            File objectStoreBase,
            Identifier objectID,
            boolean validate)
        throws TException
    {
        try {
            log(MESSAGE + "getCloudManifest entered"
                    + " - objectID=" + objectID
                    + " - validate=" + validate
                    , 10);
            ecslogger.debug("{}: getCloudManifest entered - objectID={} - validate={}", NAME, objectID, validate);
            ContentCloudManifest content = ContentCloudManifest.getContentCloudManifest(s3service, bucket, objectID, validate, logger);
            FileContent fileContent = content.callEx();
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }

    @Override
    public void getCloudManifestStream(
            File objectStoreBase,
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
            ecslogger.debug("{}: getCloudManifestStream entered - objectID={} - validate={}", NAME, objectID, validate);
            ContentCloudManifestStream content = ContentCloudManifestStream.getContentCloudManifestStream(s3service, bucket, objectID, validate, outStream, logger);
            content.callEx();

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }

    @Override
    public FileContent getVersionArchive(
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        try {
            log(MESSAGE + "getVersionArchive entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            ecslogger.debug("{}: getVersionArchive entered - objectID={} - versionID={} - archiveTypeS={} - returnIfError={}",
                    NAME, objectID, versionID, archiveTypeS, returnIfError);
            
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, objectStoreBase, archiveTypeS, logger);
            FileContent fileContent = cloudArchive.buildVersion(versionID, archiveTypeS);
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
        }


    }
    
    @Override
    public void keyToFile (
            File objectStoreBase,
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

    @Override
    public void getVersionArchiveStream(
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        try {
            log(MESSAGE + "getVersionArchive entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - archiveTypeS=" + archiveTypeS
                    + " - returnIfError=" + returnIfError
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            ecslogger.debug("{}: getVersionArchiveStream entered - objectID={} - versionID={} - archiveTypeS={} - returnIfError={}",
                    NAME, objectID, versionID, archiveTypeS, returnIfError);
            
            CloudArchive cloudArchive = new CloudArchive(s3service, bucket, objectID, objectStoreBase, archiveTypeS, logger);
            cloudArchive.buildVersion(versionID, outputStream);

        } catch (Exception ex) {
            throw makeTException(ex);
        }


    }

    @Override
    public FileContent getVersionLink(
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            String linkBaseURL,
            Boolean presign,
            Boolean update,
            String filter,
            String format)
        throws TException
     {
        try {
            log(MESSAGE + "getVersionLink entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - linkBaseURL=" + linkBaseURL
                    , 10);
            ecslogger.debug("{}: getVersionArchiveStream entered - objectID={} - versionID={} - linkBaseURL={}", NAME, objectID, versionID, linkBaseURL);
            if (logger == null) {
                System.out.println("***null logger");
            }
            ContentVersionLink versionLink 
                    = ContentVersionLink.getContentVersionLink(s3service, bucket, objectID, versionID, linkBaseURL, presign, update, filter, format, logger);
            return versionLink.callEx();

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
    @Override
    public boolean isVerifyOnRead() {
        return verifyOnRead;
    }

    /**
     * Set verifyOnRead switch
     * @param verifyOnReadS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    @Override
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
    @Override
    public boolean isVerifyOnWrite() {
        return verifyOnWrite;
    }

    /**
     * Set verifyOnWrite switch
     * @param verifyOnWwriteS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    @Override
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
        ecslogger.error(tex.toString());
        logger.logError(tex.dump(MESSAGE), 20);
        ecslogger.debug(tex.dump(MESSAGE));
        return tex;
    }
    
    @Override
    public CloudStoreInf getCloudService()
    {
        return s3service;
    }
    
    @Override
    public String getCloudBucket()
    {
        return bucket;
    }
}

