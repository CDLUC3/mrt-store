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
import org.cdlib.mrt.store.ObjectLocationInf;
import org.cdlib.mrt.store.ObjectStoreInf;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestSAX;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.action.ArchiveComponent;
import org.cdlib.mrt.store.action.ProducerComponentList;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.action.CloudArchive;
import org.cdlib.mrt.store.LastActivity;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.store.action.CopyNodeObject;
import org.cdlib.mrt.store.action.CopyVirtualObject;
import org.cdlib.mrt.store.KeyFileInf;
import org.cdlib.mrt.utility.ArchiveBuilder;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TLockFile;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Specific Node level service
 * @author dloy
 */
public class CAN
        extends CANAbs
        implements NodeInf, KeyFileInf
{

    protected static final String NAME = "CAN";
    protected static final String MESSAGE = NAME + ": ";
    public static final String CANSTATS = "summary-stats.txt";
    protected SpecScheme namasteSpec = null;

    /**
     * CAN Constructor
     * @param logger process logging
     * @param nodeID node identifier
     * @param nodeForm
     * @param objectLocation indexer to file location (e.g. pairtree)
     * @param objectStore leaf store handling (e.g. dflat)
     * @param nodeState static information about node state
     */
    protected CAN(
            LoggerInf logger,
            NodeForm nodeForm,
            ObjectLocationInf objectLocation,
            ObjectStoreInf objectStore,
            NodeState nodeState)
        throws TException
    {
        super(logger, nodeForm, objectLocation, objectStore, nodeState);
        namasteSpec = nodeState.retrieveNodeType();
        File home = nodeForm.getCanHome();
        buildNamasteFile(home);
        setNodeID(nodeForm.getNodeID());
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
        File objectLocationFile = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "addVersion - objectID required");
            }
            if ((manifestFile == null) || !manifestFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "addVersion - manifestFile missing");
            }
            
            objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectAlreadyExists = objectLocationFile.exists();
            if (!objectAlreadyExists) {
                objectLocationFile = objectLocation.buildObjectLocation(objectID);
            }
            versionState = objectStore.addVersion(objectLocationFile, objectID, manifestFile);
            versionState.setObjectID(objectID);
            updateCANStats(versionState);
            setLastActivity(LastActivity.Type.LASTADDVERSION);

            log(MESSAGE + "addVersion complete"
                + " - objectID=" + objectID
                , 10);
            return versionState;

        } catch (Exception ex) {
            System.out.println("!!!!Exception:" + ex + "Trace:" + StringUtil.stackTrace(ex));
            try {
                if ((objectLocationFile != null) && objectLocationFile.exists()) {
                    if (!objectAlreadyExists) {
                        boolean success = objectLocation.removeObjectLocation(objectID);
                        logger.logError(MESSAGE + "addVersion backout objectID=" + objectID
                                + " - success=" + success, 3);
                    }
                }
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
        File objectLocationFile = null;
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
            
            objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectAlreadyExists = objectLocationFile.exists();
            if (!objectAlreadyExists) {
                objectLocationFile = objectLocation.buildObjectLocation(objectID);
            }
            versionState = objectStore.updateVersion(objectLocationFile, objectID, manifestFile, deleteList);
            versionState.setObjectID(objectID);
            
            updateCANStats(versionState);
            setLastActivity(LastActivity.Type.LASTADDVERSION);

            log(MESSAGE + "addVersion complete"
                + " - objectID=" + objectID
                , 10);
            return versionState;

        } catch (Exception ex) {
            System.out.println("!!!!Exception:" + ex + "Trace:" + StringUtil.stackTrace(ex));
            try {
                if ((objectLocationFile != null) && objectLocationFile.exists()) {
                    if (!objectAlreadyExists) {
                        boolean success = objectLocation.removeObjectLocation(objectID);
                        logger.logError(MESSAGE + "addVersion backout objectID=" + objectID
                                + " - success=" + success, 3);
                    }
                }
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
        File objectLocationFile = null;
        File parentObject = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "deleteVersion - objectID required");
            }
            objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectAlreadyExists = objectLocationFile.exists();
            if (!objectAlreadyExists) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "deleteVersion - object not found:" + objectID);
            }
            parentObject = objectLocationFile.getParentFile();
            versionState = objectStore.deleteVersion(objectLocationFile, objectID, versionID);
            versionState.setObjectID(objectID);
            updateCANStatsDelete(versionState);
            setLastActivity(LastActivity.Type.LASTDELETEVERSION);

            log(MESSAGE + "deleteVersion complete"
                + " - objectID=" + objectID
                , 10);
            return versionState;

        } catch (Exception ex) {
            throw makeGeneralTException("deleteVersion", ex);

        } finally {
            FileUtil.deleteEmptyPath(parentObject);
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
        File objectLocationFile = null;
        File parentObject = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "deleteObject - objectID required");
            }
            objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectAlreadyExists = objectLocationFile.exists();
            if (!objectAlreadyExists) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "deleteObject - object not found:" + objectID);
            }
            parentObject = objectLocationFile.getParentFile();
            objectState = objectStore.deleteObject(objectLocationFile, objectID);
            updateCANStatsDelete(objectState);
            setLastActivity(LastActivity.Type.LASTDELETEOBJECT);
            log(MESSAGE + "deleteObject complete"
                + " - objectID=" + objectID
                , 10);
            return objectState;

        } catch (Exception ex) {
            throw makeGeneralTException("deleteObject", ex);

        } finally {
            FileUtil.deleteEmptyPath(parentObject);
        }
    }

    /**
     * Update CAN stats for addVersion
     * @param versionState version state of added version
     * @throws TException process exception
     */
    protected void updateCANStats(VersionState versionState)
            throws TException
    {
        TLockFile fileStatsLock = null;
        try {
            File home = nodeForm.getCanHome();
            File log = new File(home, "log");
            if (!log.exists()) {
                log.mkdir();
            }
            fileStatsLock = getFileStatsLockException(log);
            File statsFile = new File(log, CANSTATS);
            CANStats stats = new CANStats(statsFile);
            if (versionState.getIdentifier() == 1) {
                stats.bump(CANStats.OBJECTCNT);
            }
            stats.bump(CANStats.VERSIONCNT);
            stats.bump(CANStats.FILECNT, versionState.getDeltaNumFiles());
            stats.bump(CANStats.TOTALSIZE, versionState.getDeltaSize());
            stats.bump(CANStats.COMPONENTFILECNT, versionState.getNumFiles());
            stats.bump(CANStats.COMPONENTTOTALSIZE, versionState.getTotalSize());
            stats.saveTable(statsFile);

        } catch (Exception ex) {
            String msg = "updateCANStats";
            throw makeGeneralTException(msg, ex);

        } finally {
            try {
                if (fileStatsLock != null) {
                    fileStatsLock.remove();
                }
            } catch (Exception e) {}
        }

    }

    /**
     * Update CAN stats for deleteVersion
     * @param versionState version state of deleted version
     * @throws TException process exception
     */
    protected void updateCANStatsDelete(VersionState versionState)
            throws TException
    {
        TLockFile fileStatsLock = null;
        try {
            File home = nodeForm.getCanHome();
            File log = new File(home, "log");
            if (!log.exists()) return;
            fileStatsLock = getFileStatsLockException(log);
            File statsFile = new File(log, CANSTATS);
            CANStats stats = new CANStats(statsFile);
            if (versionState.getIdentifier() == 1) {
                stats.bump(CANStats.OBJECTCNT, -1);
            }
            stats.bump(CANStats.VERSIONCNT, -1);
            stats.bump(CANStats.FILECNT, versionState.getDeltaNumFiles());
            stats.bump(CANStats.TOTALSIZE, versionState.getDeltaSize());
            stats.bump(CANStats.COMPONENTFILECNT, -versionState.getNumFiles());
            stats.bump(CANStats.COMPONENTTOTALSIZE, -versionState.getTotalSize());
            stats.saveTable(statsFile);

        } catch (Exception ex) {
            String msg = "updateCANStatsDelete";
            throw makeGeneralTException(msg, ex);

        } finally {
            try {
                if (fileStatsLock != null) {
                    fileStatsLock.remove();
                }
            } catch (Exception e) {}
        }

    }

    /**
     * Update CAN Stats for deleteObject
     * @param objectStatsSize size of deleted object
     * @param objectStatsNumFile number files of deleted object
     * @param numVersions number versions of deleted object
     * @throws TException
     */
    protected void updateCANStatsDelete(
            long objectStatsSize,
            long objectStatsNumFile,
            int numVersions)
        throws TException
    {
        TLockFile fileStatsLock = null;
        try {
            File home = nodeForm.getCanHome();
            File log = new File(home, "log");
            if (!log.exists()) return;
            fileStatsLock = getFileStatsLockException(log);
            File statsFile = new File(log, CANSTATS);
            CANStats stats = new CANStats(statsFile);
            stats.bump(CANStats.OBJECTCNT, -1);
            stats.bump(CANStats.VERSIONCNT, -numVersions);
            stats.bump(CANStats.FILECNT, -objectStatsNumFile);
            stats.bump(CANStats.TOTALSIZE, -objectStatsSize);
            stats.saveTable(statsFile);

        } catch (Exception ex) {
            String msg = "updateCANStatsDelete";
            throw makeGeneralTException(msg, ex);

        } finally {
            try {
                if (fileStatsLock != null) {
                    fileStatsLock.remove();
                }
            } catch (Exception e) {}
        }

    }

    /**
     * Update CAN Stats for deleteObject
     * @param objectStatsSize size of deleted object
     * @param objectStatsNumFile number files of deleted object
     * @param numVersions number versions of deleted object
     * @throws TException
     */
    protected void updateCANStatsDelete(
            ObjectState objectState)
        throws TException
    {
        long objectStatsSize = objectState.getTotalActualSize();
        long objectStatsNumFile = objectState.getNumActualFiles();
        int numVersions = objectState.getNumVersions();
        long objectComponentSize = objectState.getSize();
        long objectComponentNumFile = objectState.getNumFiles();
        TLockFile fileStatsLock = null;
        try {
            File home = nodeForm.getCanHome();
            File log = new File(home, "log");
            if (!log.exists()) return;
            fileStatsLock = getFileStatsLockException(log);
            File statsFile = new File(log, CANSTATS);
            CANStats stats = new CANStats(statsFile);
            stats.bump(CANStats.OBJECTCNT, -1);
            stats.bump(CANStats.VERSIONCNT, -numVersions);
            stats.bump(CANStats.FILECNT, -objectStatsNumFile);
            stats.bump(CANStats.TOTALSIZE, -objectStatsSize);
            stats.bump(CANStats.COMPONENTFILECNT, -objectComponentNumFile);
            stats.bump(CANStats.COMPONENTTOTALSIZE, -objectComponentSize);
            stats.saveTable(statsFile);

        } catch (Exception ex) {
            String msg = "updateCANStatsDelete";
            throw makeGeneralTException(msg, ex);

        } finally {
            try {
                if (fileStatsLock != null) {
                    fileStatsLock.remove();
                }
            } catch (Exception e) {}
        }

    }


    /**
     * Add date LastAdd date property to last-activity.txt
     * @throws TException process exception
     */
    protected void setLastActivity(LastActivity.Type key)
            throws TException
    {
        TLockFile fileStatsLock = null;
        try {
            LastActivity lastActivity = getLastActivityFile();
            lastActivity.setPropery(key);
            lastActivity.writeFile();

        } catch (Exception ex) {
            throw makeGeneralTException("setLastActivity", ex);

        } finally {
            try {
                if (fileStatsLock != null) {
                    fileStatsLock.remove();
                }
            } catch (Exception e) {}
        }
    }


    /**
     * Add date LastVersionDelete date property to last-activity.txt
     * @throws TException process exception
     */
    protected LastActivity getLastActivityFile()
            throws TException
    {
        try {
            File home = nodeForm.getCanHome();
            File log = new File(home, "log");
            if (!log.exists()) {
                log.mkdir();
            }
            File lastActivityFile = new File(log, "last-activity.txt");
            return new LastActivity(lastActivityFile);

        } catch (Exception ex) {
            throw makeGeneralTException("getLastActivityFile", ex);
        }
    }

    protected void incFileCnt(File base, String fileName, int cnt)
        throws TException
    {
        try {
            File currFile = new File(base, fileName);
            FileUtil.incFile(currFile, cnt);
            logger.logMessage(MESSAGE + "incFileCnt"
                    + " - file=" + currFile.getName()
                    , LoggerInf.LogLevel.DEBUG);

        } catch (Exception ex) {
            throw makeGeneralTException("incFileCnt", ex);
        }
    }

    protected TLockFile getFileStatsLockException(File logDirect)
            throws TException
    {
        TLockFile fileLock = null;
        try {
            fileLock =
                TLockFile.getReplaceTLockFile(
                        logDirect,
                        "lock.txt",
                        60, 60, 3, 3);
            if (!fileLock.isActiveLock()) {
                String msg = MESSAGE
                    + "Object Lock found - processing terminated";
                throw new TException.CONCURRENT_UPDATE( msg);
            }
            return fileLock;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Exception attempting to lock file - Exception:" + ex;
            logger.logError(err, LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex), LoggerInf.LogLevel.DEBUG);
            throw new TException.LOCKING_ERROR( err);

        }
    }

    @Override
    public FileContent getVersionLink(
            Identifier objectID,
            int versionID,
            String linkBaseURL)
        throws TException
    {
        FileContent manifestFile = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - objectID required");
            }

            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist");
            }
            if (StringUtil.isEmpty(linkBaseURL)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - linkBaseURL not supplied");
            }

            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            manifestFile = objectStore.getVersionLink(objectLocationFile, objectID, versionID, linkBaseURL);

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

            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Object not found");
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);


            versionState = objectStore.getVersionState(objectLocationFile, objectID, versionID);
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

            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Object not found:" + objectID);
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);


            objectState = objectStore.getObjectState(objectLocationFile, objectID);
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

            if (!objectLocation.objectExists(objectID)) {
                return null;
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);


            ComponentContent componentContent = objectStore.getVersionContent(objectLocationFile, objectID, versionID);
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist");
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFileState - objectID required");
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            return objectStore.getFileState(objectLocationFile, objectID, versionID, fileName);

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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist:" + objectID.getValue());
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - fileName required for " + objectID.getValue());
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            File returnFile = objectStore.getFile(objectLocationFile, objectID, versionID, fileName);
            File tempFile = returnFile;
            if (!isTempCldFile(returnFile)) {
                tempFile = FileUtil.copy2Temp(returnFile);
            }
            FileComponent fileState = objectStore.getFileState(objectLocationFile, objectID, versionID, fileName);
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
            
            
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectStore.keyToFile(objectLocationFile, key, outFile);
            
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist:" + objectID.getValue());
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFile - fileName required for " + objectID.getValue());
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectStore.getFileStream(objectLocationFile, objectID, versionID, fileName, outStream);

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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist");
            }
            if (StringUtil.isEmpty(fileName)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFileState - objectID required");
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            FileFixityState fileFixityState = objectStore.getFileFixityState(
                    objectLocationFile, objectID, versionID, fileName);
            fileFixityState.setObjectID(objectID);
            fileFixityState.setVersionID(versionID);
            fileFixityState.setFileName(fileName);
            return fileFixityState;

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
        File nodeHome = nodeForm.getCanHome();
        if ((nodeHome == null) || !nodeHome.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM (
                    MESSAGE + "getNodeState - missing nodeHome");
        }
        if (nodeState == null) {
            throw new TException.INVALID_OR_MISSING_PARM (
                    MESSAGE + "getNodeState - missing nodeState");
        }
        NodeState currentNodeState = new NodeState(nodeState, nodeHome);
        LastActivity lastActivity = getLastActivityFile();
        currentNodeState.setLastAddVersion(lastActivity.getDate(LastActivity.Type.LASTADDVERSION));
        currentNodeState.setLastDeleteObject(lastActivity.getDate(LastActivity.Type.LASTDELETEOBJECT));
        currentNodeState.setLastDeleteVersion(lastActivity.getDate(LastActivity.Type.LASTDELETEVERSION));
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        "Object does not exist - objectID=" + objectID);
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            FileContent objectArchive = objectStore.getObject(
                    objectLocationFile,
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        "Object does not exist - objectID=" + objectID);
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectStore.getObjectStream(
                    objectLocationFile,
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
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            FileContent objectArchive = objectStore.getCloudManifest(
                    objectLocationFile, objectID, validate);
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
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectStore.getCloudManifestStream(
                    objectLocationFile, objectID, validate, outStream);

        } catch (Exception ex) {
            throw makeGeneralTException("getObjectArchive", ex);
        }
    }

    @Override
    public FileContent getObjectLink(
            Identifier objectID,
            String linkBaseURL)
        throws TException
    {
        FileContent manifestFile = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - objectID required");
            }

            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist - objectID=" + objectID);
            }
            if (StringUtil.isEmpty(linkBaseURL)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - linkBaseURL not supplied");
            }

            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            manifestFile = objectStore.getObjectLink(objectLocationFile, objectID, linkBaseURL);

            log(MESSAGE + "getVersionLink entered"
                + " - objectID=" + objectID
                + " - baseURL=" + linkBaseURL
                , 10);
            return manifestFile;

         } catch (Exception ex) {
            throw makeGeneralTException("getObjectLink", ex);
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist");
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            FileContent versionArchive = objectStore.getVersionArchive(
                    objectLocationFile, objectID, versionID, returnIfError, archiveTypeS);
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist");
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            FileContent objectManifest = objectStore.getCloudManifest(
                    objectLocationFile, objectID, false);
            VersionMap versionMap = getVersionMap(objectManifest.getFile());
            
            int nameVer = versionID;
            if (nameVer == 0) nameVer = versionMap.getCurrent();
            String archiveName = getArchiveName(objectID, nameVer);
            File baseDir = FileUtil.getTempDir(archiveName);
            CloudStoreInf s3service = objectStore.getCloudService();
            String bucket = objectStore.getCloudBucket();
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist");
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            FileContent objectManifest = objectStore.getCloudManifest(
                    objectLocationFile, objectID, false);
            VersionMap versionMap = getVersionMap(objectManifest.getFile());
            
            int nameVer = versionID;
            if (nameVer == 0) nameVer = versionMap.getCurrent();
            String archiveName = getArchiveName(objectID, nameVer);
            File baseDir = FileUtil.getTempDir(archiveName);
            CloudStoreInf s3service = objectStore.getCloudService();
            String bucket = objectStore.getCloudBucket();
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
            if (!objectLocation.objectExists(objectID)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        MESSAGE + "Object does not exist");
            }
            File objectLocationFile = objectLocation.getObjectLocation(objectID);
            objectStore.getVersionArchiveStream(
                    objectLocationFile, objectID, versionID, returnIfError, archiveTypeS, outputStream);

         } catch (Exception ex) {
            throw makeGeneralTException("getVersionArchiveStream", ex);
         }
    }

    protected TException makeGeneralTException(String header, Exception ex)
    {
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