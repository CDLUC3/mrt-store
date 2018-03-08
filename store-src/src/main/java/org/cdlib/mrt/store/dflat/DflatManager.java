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

package org.cdlib.mrt.store.dflat;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Vector;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowObject;
import org.cdlib.mrt.core.ManifestRowAdd;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.utility.ArchiveBuilder;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.DirectoryStats;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.MessageDigestType;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TLockFile;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PairtreeUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.store.LastActivity;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.core.Tika;
/**
 * Used for managing all Dflat functions
 * @author dloy
 */
public class DflatManager
{
    protected static final String NAME = "DflatManager";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String NL =  System.getProperty("line.separator");

    public static final boolean NULL_MESSAGE_DIGEST_ALLOWED = false;
    public static final String DFLATSTATS = "summary-stats.txt";
    public static final String MANIFESTTXT = "manifest.txt";


    protected LoggerInf logger = null;
    protected File m_objectStoreBase = null;
    protected DflatVersionManagerAbs m_dflatVersionManager = null;
    protected boolean DEBUG = false;
    protected DflatInfo dflatInfo = null;
    protected boolean verifyOnRead = true;
    protected boolean verifyOnWrite = true;
    protected SpecScheme namasteSpec = null;
    protected Tika tika = null;
    protected long contentAccumTime = 0;
    protected long validateAccumTime = 0;

    /**
     * Get the current Dflat version handler
     * The version managers handle the specifics of Dflat handling at the versioning
     * level. At this point only Redd handling is supported
     * @return Dflat version handler
     */
    public DflatVersionManagerAbs getDflatVersionManager() {
        return m_dflatVersionManager;
    }

    /**
     * set debug on or off
     * @param debugDump debug on or off
     */
    public void setDebugDump(boolean debugDump) {
        this.DEBUG = debugDump;
    }

    /**
     * Constructor: DflatManager
     * @param objectStoreBase bas object store location (e.g. after pairtree)
     * @param logger process logger
     * @param dflatInfo Dflat characteristics
     * @throws TException
     */
    public DflatManager(
            File objectStoreBase,
            LoggerInf logger,
            DflatInfo dflatInfo,
            boolean verifyOnRead,
            boolean verifyOnWrite)
        throws TException
    {
        try {
            if (!objectStoreBase.exists()) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Requested object not found");
            }

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to resolve ObjectStore base: Exception" + ex);
        }
        this.logger = logger;
        this.dflatInfo = dflatInfo;
        this.verifyOnRead = verifyOnRead;
        this.verifyOnWrite = verifyOnWrite;
        m_objectStoreBase = objectStoreBase;
        SpecScheme deltaSpec = dflatInfo.getSpecScheme("delta", DflatInfo.DELTASCHEME);
        switch(deltaSpec.getScheme()) {
            case redd_1d0:
                m_dflatVersionManager = DflatVersionManagerAbs.getDflatVersionReddManager(objectStoreBase, logger, dflatInfo);
                break;
            }
        if (m_dflatVersionManager == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to get DflatManager");
        }
        namasteSpec = dflatInfo.getSpecScheme("leaf", DflatInfo.LEAFSCHEME);
        tika = Tika.getTika(logger);
    }

    /**
     * Add new version
     * @param objectID of version to be added
     * @param manifestInputStream stream for dflatpost manifest
     * @return VersionState for add function
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionState addVersion (
                Identifier objectID,
                InputStream manifestInputStream)
        throws TException
    {        
        TLockFile fileLock  = getDflatLockException();
        contentAccumTime = 0;
        validateAccumTime = 0;
        try {
            log(MESSAGE + "putVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            if ((objectID == null) || (manifestInputStream == null)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "Manifest file does not exist");
            }

            buildDflatInfoFile();
            buildDflatNamasteFile();
            long startTime = DateUtil.getEpochUTCDate();
            Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
            VersionContent inVersionState = DflatUtil.getVersionContent(
                    logger,
                    manifest,
                    manifestInputStream);
            long contentTime = DateUtil.getEpochUTCDate();
            VersionState versionState = addVersionContent(inVersionState);
            long outputTime = DateUtil.getEpochUTCDate();
            String isoDate = DateUtil.getCurrentIsoDate();
            logger.logMessage("***addVersion timing[" + objectID.getValue() + "," + isoDate + "]:"
                    + " - file=" + contentAccumTime
                    + " - validate=" + validateAccumTime
                    + " - trans=" + (outputTime - startTime),
                    15
            );
            return versionState;

        } catch(Exception ex) {
            throw makeTException("Could not complete add version", ex);
            
        } finally {
            removeLock(fileLock);
        }
    }

    /**
     * Add new version
     * @param objectID of version to be added
     * @param manifestInputStream stream for dflatpost manifest
     * @return VersionState for add function
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionState updateVersion (
                Identifier objectID,
                InputStream manifestInputStream,
                String [] deleteList)
        throws TException
    {        
        TLockFile fileLock  = getDflatLockException();
        try {
            log(MESSAGE + "putVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            buildDflatInfoFile();
            buildDflatNamasteFile();
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "objectID does not exist");
            }
            VersionContent updateVersionContent = null;
            if (manifestInputStream != null) {
                Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
                updateVersionContent = DflatUtil.getVersionContent(
                    logger,
                    manifest,
                    manifestInputStream);
                updateVersionContent.setObjectID(objectID);
            }
            VersionContent addContent = getAddContentFromUpdate(objectID, updateVersionContent, deleteList);
            if (DEBUG) {
                List<FileComponent> fileComponents = addContent.getFileComponents();
                for (FileComponent component : fileComponents) {
                    System.out.println("updateVersion component name=" + component.getIdentifier());
                }
            }
            return addVersionContent(addContent);

        } catch(Exception ex) {
            throw makeTException("Could not complete add version", ex);
            
        } finally {
            removeLock(fileLock);
        }
    }

    protected VersionContent getAddContentFromUpdate(
                Identifier objectID,
                VersionContent updateVersionContent,
                String [] deleteList)
            throws TException
    {
        try {
            VersionContent currentVersionContent = getCurrentVersionContent();
            
            List<FileComponent> currentComponents = null;
            if (currentVersionContent != null) {
                currentComponents = currentVersionContent.getFileComponents();
                addComponentFiles(currentComponents);
            }
            List<FileComponent> updateComponents = null;
            if (updateVersionContent != null) {
                updateComponents = updateVersionContent.getFileComponents();
            }
            if ((currentComponents == null) && (updateComponents == null)) {
                throw new TException.REQUEST_INVALID(MESSAGE + "Initial version requires update components");
            }
            List<FileComponent> addComponents = VersionMap.getMergeComponents(
                    updateComponents,
                    currentComponents,
                    deleteList);
            ComponentContent componentContent = new ComponentContent(addComponents);
            VersionContent addVersionContent = new VersionContent(componentContent);
            addVersionContent.setObjectID(objectID);
            return addVersionContent;
            
        } catch(Exception ex) {
            throw makeTException("Could not complete add version", ex);
            
        }
    }

    protected void addComponentFiles(
                List<FileComponent> components)
            throws TException
    {
        try {
            if (DEBUG) System.out.println("addComponentFiles - next version:" + m_dflatVersionManager.getNextVersion());
            for (FileComponent component : components) {
                if (DEBUG) System.out.println(component.dump("addComponentFiles"));
                File compFile = getFile(-1, component.getIdentifier());
                component.setComponentFile(compFile);
                if (DEBUG) if (compFile!=null)System.out.println("addComponentFiles FILE:" + compFile.getCanonicalPath());
            }
            
        } catch(Exception ex) {
            throw makeTException("Could not complete add version", ex);
            
        }
    }
    
    /**
     * Using the VersionContent build new version
     * @param versionContent contains all file components
     * @return VersionState for add Function
     * @throws TException 
     */
    protected VersionState addVersionContent ( 
            VersionContent versionContent)
        throws TException
    {      
        try {
            File tempVersion = createNewVersion(
                m_objectStoreBase,
                versionContent);
            m_dflatVersionManager.setCurrent(tempVersion);
            setLogAddVersion();
            DirectoryStats endStats = FileUtil.getDirectoryStats(m_objectStoreBase);
            //System.out.println("***BEGIN*** endStats - fileCnt=" + endStats.fileCnt + " - fileSize=" + endStats.fileSize);
            VersionState versionState = getCurrentVersionState();
            setVersionStats(versionState, endStats, versionState.getIdentifier());
            return versionState;

        } catch(Exception ex) {
            throw makeTException("Could not complete add version", ex);
        }
    }

    /**
     * Remove current version from an object
     * @param objectID of version to be added
     * @return VersionState for current function
     * @throws org.cdlib.mrt.utility.TException
     */
    public VersionState deleteVersion (
                Identifier objectID,
                int versionID)
        throws TException
    {
        TLockFile fileLock  = getDflatLockException();
        try {
            VersionState versionState = getCurrentVersionState();
            int currentVersionID = testVersionCurrent(-1);
            int previousVersionID = currentVersionID - 1;
            if ((versionID > 0) && (currentVersionID != versionID)) {
                throw new TException.REQUEST_ELEMENT_UNSUPPORTED(
                        MESSAGE + "only current version supported for deleteVersion");
            }
            log(MESSAGE
                    + " - objectID=" + objectID
                    + " - deleteVersion currentVersionID=" + currentVersionID
                    + " - previousVersionID=" + previousVersionID
                    , 10);
            if (currentVersionID == 1) {
                DirectoryStats endStats = FileUtil.getDirectoryStats(m_objectStoreBase);
                versionState.setDeltaSize(-1*endStats.fileSize);
                versionState.setDeltaNumFiles(-1*endStats.fileCnt);
                FileUtil.deleteDir(m_objectStoreBase);
                return versionState;
            }
            File previousDeltaVersion = m_dflatVersionManager.getVersionNameFile(previousVersionID);
            if (!previousDeltaVersion.exists()) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE
                        + "previous delta version exists");
            }
            File previousFullVersion = new File(m_objectStoreBase, "newV");
            getFullVersion(previousVersionID, previousFullVersion);
            m_dflatVersionManager.replaceCurrentWithPrevious(currentVersionID, previousFullVersion);
            DirectoryStats endStats = FileUtil.getDirectoryStats(m_objectStoreBase);
            setVersionStats(versionState, endStats, (versionState.getIdentifier() - 1));
            setLogVersionDelete();
            return versionState;

        } catch(Exception ex) {
            throw makeTException("Could not complete version delete", ex);

        } finally {
            removeLock(fileLock);
        }
    }


    /**
     * Set local version stats file values - summary-stats.txt
     * @param versionState contains update state count information
     * @param endStats current stat file tallies
     * @throws TException
     */
    protected void setVersionStats(
            VersionState versionState,
            DirectoryStats endStats,
            int currentVersion)
        throws TException
    {
        File log = new File(m_objectStoreBase, "log");
        if (!log.exists()) {
            log.mkdir();
        }
        File statsFile = new File(log, DFLATSTATS);
        DflatStats stats = new DflatStats(statsFile);
        long preDirSize = stats.getCount(DflatStats.TOTALSIZE);
        long preDirCnt = stats.getCount(DflatStats.FILECNT);

        stats.set(DflatStats.TOTALSIZE, endStats.fileSize);
        stats.set(DflatStats.VERSIONCNT, currentVersion);
        stats.set(DflatStats.FILECNT, endStats.fileCnt);

        long deltaSize = endStats.fileSize - preDirSize;
        versionState.setDeltaSize(deltaSize);
        long deltaCnt = endStats.fileCnt - preDirCnt;
        versionState.setDeltaNumFiles(deltaCnt);
        if (DEBUG) {
            System.out.println("***setVersionState:"
                    + " - preDirSize=" + preDirSize
                    + " - preDirCnt=" + preDirCnt
                    + " - endStats.fileSize=" + endStats.fileSize
                    + " - endStats.fileCnt=" + endStats.fileCnt
                    + " - versionState.getDeltaSize=" + versionState.getDeltaSize()
                    + " - versionState.getDeltaNumFiles=" + versionState.getDeltaNumFiles()
                    );
        }
        stats.saveTable(statsFile);
    }


    /**
     * Set local version stats file values - summary-stats.txt
     * @param versionState contains update state count information
     * @param endStats current stat file tallies
     * @throws TException
     */
    /**
     * Return a Stats table for this object
     * @return Stats table
     * @throws TException
     */
    protected DflatStats getDflatStats()
        throws TException
    {
        File log = new File(m_objectStoreBase, "log");
        if (!log.exists()) {
            log.mkdir();
        }
        File statsFile = new File(log, DFLATSTATS);
        return new DflatStats(statsFile);
    }

    /**
     * Get a dynamic version state including tallies for this version content
     * @param versionContent passed manifest content for a version
     * @return VersionState based off of manifest data
     * @throws TException process exception
     */
    protected VersionState getVersionState(VersionContent versionContent)
        throws TException
    {
        try {
            if (versionContent == null) {
                String msg = MESSAGE
                    + "getVersionState - versionContent is missing.";
                throw new TException.INVALID_OR_MISSING_PARM(msg);
            }


            VersionState versionState = new VersionState();
            versionState.setFileNames(versionContent);
            setVersionStateCnts(versionContent, versionState);
            versionState.setCreated(getFirstComponentDate(versionContent));

            return versionState;

        } catch (Exception ex) {
            throw makeTException("Unable to build VersionState.", ex);
        }
    }

    /**
     * Return the date from the first manifest component
     * @param versionContent list of file components for this version
     * @return date of first manifest component
     */
    protected DateState getFirstComponentDate(VersionContent versionContent)
    {
        if (versionContent == null) return null;
        Vector<FileComponent> fileComponents = versionContent.getFileComponents();
        if ((fileComponents == null) || (fileComponents.size() == 0)) return null;
        FileComponent component = fileComponents.get(0);
        return component.getCreated();
    }

    /**
     * tally VersionContent information and save tally in VersionState
     * @param versionContent manifest content data
     * @param versionState Version State information
     */
    protected void setVersionStateCnts(
            VersionContent versionContent,
            VersionState versionState)
        throws TException
    {
            long sumSize = 0;
            int sumFileCnt = 0;
            Vector<FileComponent> fileStates = versionContent.getFileComponents();
            for (FileComponent fileState: fileStates) {
                sumSize += fileState.getSize();
                sumFileCnt++;
            }
            versionState.setNumFiles(sumFileCnt++);
            versionState.setTotalSize(sumSize);
            File fileVersion = m_dflatVersionManager.getVersionFile(versionContent.getVersionID());
            DirectoryStats endStats = FileUtil.getDirectoryStats(fileVersion);
            versionState.setNumActualFiles((long)endStats.getFileCnt());
            versionState.setTotalActualSize(endStats.getFileSize());
    }

    /**
     * get all file states for a specific version using versionID
     * @param versionID version identifier for this manifest
     * @return VersionContent contains list and hash of file states this version
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionContent getVersionContentFromManifestFile(int versionID)
        throws TException
    {
        FileInputStream inputStream = null;
        try {
            int currentVersionID = testVersionCurrent(versionID);
            if (versionID < 1) versionID = currentVersionID;
            File manifestFile = getVersionManifest(versionID);
            return getVersionContentFromManifestFile(versionID, manifestFile);

        } catch (Exception ex) {
            logger.logError("getVersionContentFromManifestFile trace" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Unable to build VersionState. Exception:" + ex);
        } finally {
            try {
                inputStream.close();
            } catch (Exception ex) {

            }
        }
    }

    /**
     * get all file states for a specific version using File
     * @param versionID version identifier for this manifest
     * @param manifestFile manifest file to be converted to VersionContent
     * @return VersionContent contains list and hash of file states this version
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionContent getVersionContentFromManifestFile(
            int versionID,
            File manifestFile)
        throws TException
    {
        FileInputStream inputStream = null;
        try {
            if ((manifestFile == null) || !manifestFile.exists()) {
                String msg = MESSAGE
                    + "getVersionContentFromManifestFile - manifest file missing.";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }

            int currentVersionID = testVersionCurrent(versionID);
            if (versionID < 1) versionID = currentVersionID;
            inputStream = new FileInputStream(manifestFile);
            Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.object);
            VersionContent versionState = DflatUtil.getVersionContent(logger, manifest, inputStream);
            versionState.setVersionID(versionID);
            return versionState;

        } catch (Exception ex) {
            logger.logError("getVersionContentFromManifestFile trace" + StringUtil.stackTrace(ex),10);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Unable to build VersionState. Exception:" + ex);
        } finally {
            try {
                inputStream.close();
            } catch (Exception ex) {

            }
        }
    }

    /**
     * Reconstruct a full version from deltas
     * @param buildVersion
     * @param nextVersion - next version to be used for reconstruction
     * @throws org.cdlib.mrt.utility.MException
     */
    public void validateFullVersion(
            int versionID,
            File fullVersion)
        throws TException
    {
        try {
            int currentVersionID = testVersionCurrent(versionID);
            if (versionID < 1) versionID = currentVersionID;
            if ((fullVersion == null) || !fullVersion.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "validateFullVersion - fullVersion not defined");
            }
            File manifestFile = new File(fullVersion,
                    DflatVersionManagerAbs.MANIFEST_TXT);
            if (!manifestFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "validateFullVersion - manifestFile file not defined");
            }
            File fullDir = new File(fullVersion, DflatVersionManagerAbs.FULL);
            if (!fullDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "validateFullVersion - fullDir file not defined");
            }

            VersionContent content
                    = getVersionContentFromManifestFile(versionID, manifestFile);
            if (content == null) {
                throw new TException.INVALID_ARCHITECTURE (
                        MESSAGE + "validateFullVersion - VersionContent not found");
            }
            Vector<FileComponent> fileStates = content.getFileComponents();
            FileComponent fileState = null;
            for (int i=0; i < fileStates.size(); i++) {
                fileState = fileStates.get(i);
                String fileID = fileState.getIdentifier();
                File testFile = new File(fullDir, fileID);
                log("***validateFullVersion - manifest test:" + fileID, 10);
                if (!testFile.exists()) {
                    throw new TException.INVALID_ARCHITECTURE (
                        MESSAGE + "validateFullVersion - manifest file missing:" + fileID);
                }
            }
            log("validateFullVersion(" + versionID +"): manifest validation OK for "
                    + fileStates.size() + " files", 10);
            Vector<File> fileList = new Vector<File>();
            File dirFile = null;
            FileUtil.getDirectoryFiles(fullDir, fileList);
            String fullDirName = fullDir.getCanonicalPath();
            for (int i=0; i < fileList.size(); i++) {
                dirFile = fileList.get(i);
                String dirFileName = dirFile.getCanonicalPath();
                dirFileName = dirFileName.substring(fullDirName.length()+1);
                dirFileName = dirFileName.replace("\\", "/");
                //log("***validateFullVersion - directory test:" + dirFileName, 10);
                fileState = content.getFileComponent(dirFileName);
                if (fileState == null) {
                    throw new TException.INVALID_ARCHITECTURE (
                        MESSAGE + "validateFullVersion - file missing from manifest:" + dirFileName);
                }
            }
            log("validateFullVersion(" + versionID +"): file validation OK for "
                    + fileList.size() + " files", 10);

        } catch (TException mex) {
            throw mex;

        } catch (Exception ex) {
            throw makeTException("buildFullPrevious", ex);
        }
    }


    /**
     * Perform fixity of the manifest.txt against full version
     * @param versionID versionID for this full version
     * @param fullVersion full version file to be tested
     * @throws org.cdlib.mrt.utility.MException
     */
    public void fixityFullVersion(
            int versionID,
            File fullVersion)
        throws TException
    {
        FileComponent fileState = null;
        try {
            if ((fullVersion == null) || !fullVersion.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "validateFullVersion - fullVersion not defined");
            }
            File manifestFile = new File(fullVersion,
                    DflatVersionManagerAbs.MANIFEST_TXT);
            if (!manifestFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "validateFullVersion - manifestFile file not defined");
            }
            File fullDir = new File(fullVersion, DflatVersionManagerAbs.FULL);
            if (!fullDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "validateFullVersion - fullDir file not defined");
            }
            int currentVersionID = testVersionCurrent(versionID);
            if (versionID < 1) versionID = currentVersionID;

            VersionContent content
                    = getVersionContentFromManifestFile(versionID, manifestFile);
            if (content == null) {
                throw new TException.INVALID_ARCHITECTURE (
                        MESSAGE + "validateFullVersion - VersionContent not found");
            }
            Vector<FileComponent> fileStates = content.getFileComponents();
            for (int i=0; i < fileStates.size(); i++) {
                fileState = fileStates.get(i);
                fixityRow(fullDir, fileState);
            }
            setLogFixity();
            log("fixityFullVersion(" + versionID +"): fixity validation OK for "
                    + fileStates.size() + " files", 10);

        } catch (Exception ex) {
            String dispName = "unknown";
            if (fileState != null) {
                dispName = fileState.getIdentifier();
            }
            String msg = "Fixity fails:"
                    + " - versionID=" + versionID
                    + " - name=" + dispName
                    + " - Exception=" + ex
                    ;
            throw new TException.FIXITY_CHECK_FAILS(msg);
        }
    }

    /**
     * Get manifest file information from current version
     * @return current version content - all file states
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionContent getCurrentVersionContent()
        throws TException
    {
        int versionID = m_dflatVersionManager.getNextVersion();
        if (versionID == 1) return null;
        versionID--;
        return getVersionContentFromManifestFile(versionID);
    }

    /**
     * @return current version state
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionState getCurrentVersionState()
        throws TException
    {
        int versionID = m_dflatVersionManager.getNextVersion();
        if (versionID == 1) return null;
        versionID--;
        return getVersionState(versionID);
    }

    /**
     * Get the VersionState for this versionID
     * @param versionID needed to be retrieved - if less than 1 then treat as current
     * @return VersionState for this versionID
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionState getVersionState(int versionID)
        throws TException
    {
        int currentVersionID = testVersionCurrent(versionID);
        if (versionID < 1) versionID = currentVersionID;
        VersionContent versionContent = getVersionContentFromManifestFile(versionID);
        VersionState versionState = getVersionState(versionContent);
        versionState.setIdentifier(versionID);
        if (currentVersionID == versionID) {
            versionState.setCurrent(true);
        }
        return versionState;
    }

    /**
     * @return ObjectState this Object
     * @throws org.cdlib.mrt.utility.MException
     */
    public ObjectState getObjectState(Identifier objectID)
        throws TException
    {
        int currentVersionID = m_dflatVersionManager.getNextVersion() - 1;
        int sumFiles = 0;
        long sumSize = 0;

        ObjectState objectState
                = ObjectState.getObjectState(objectID);
        for (int i = 1; i <= currentVersionID; i++) {
            VersionState versionState = getVersionState(i);
            sumFiles += versionState.getNumFiles();
            sumSize += versionState.getTotalSize();
            objectState.addVersion(versionState);
        }
        objectState.setNumFiles(sumFiles);
        objectState.setSize(sumSize);
        objectState.setNumVersions(currentVersionID);
        DflatStats stats = getDflatStats();
        long statsNumSize = stats.getCount(DflatStats.TOTALSIZE);
        long statsNumFiles = stats.getCount(DflatStats.FILECNT);
        objectState.setNumActualFiles(statsNumFiles);
        objectState.setTotalActualSize(statsNumSize);
        LastActivity lastActivity = getLastActivityFile();
        objectState.setLastAddVersion(lastActivity.getDate(LastActivity.Type.LASTADDVERSION));
        objectState.setLastDeleteVersion(lastActivity.getDate(LastActivity.Type.LASTDELETEVERSION));
        objectState.setLastFixity(lastActivity.getDate(LastActivity.Type.LASTFIXITY));
        return objectState;
    }

    /**
     * Build a full version from a dflat manifest
     * @param baseFile base file to contain a tempVersion directory with full content
     * @param versionContent manifest content
     * @return version file directory with full content
     * @throws TException
     */
    protected File createNewVersion(
            File baseFile,
            VersionContent versionContent)
        throws TException
    {
        ManifestRowAbs.ManifestType manifestType = ManifestRowAbs.ManifestType.object;
        Manifest manifestDflat = null;
        try {
            if ((baseFile == null) || !baseFile.exists()) {
                String msg = MESSAGE
                    + "createNewVersion - missing base directory.";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }
            File tempVersion = new File(baseFile, "tempVersion");
            if (tempVersion.exists()) {
                FileUtil.deleteDir(tempVersion);
            }
            tempVersion.mkdir();
            File fullDirectory = new File(tempVersion, "full");
            fullDirectory.mkdir();

            log(MESSAGE + "createNewVersion fullDirectory created:" + fullDirectory.getCanonicalPath(), 10);
            Vector<FileComponent> fileStates  = versionContent.getFileComponents();
            ManifestRowObject rowOut
                    = (ManifestRowObject)ManifestRowAbs.getManifestRow(manifestType, logger);
            manifestDflat = Manifest.getManifest(logger, manifestType);

            File manifestFile = new File(tempVersion, "manifest.txt");
            manifestDflat.openOutput(manifestFile);

            //ObjectManifest versionManifest = getDflatManifest(fullDirectory);
            int outCnt = 0;
            for (FileComponent fileComponent : fileStates) {
                fileComponent.setCreated();
                addRow(fullDirectory, fileComponent);
                rowOut.setFileComponent(fileComponent);
                dumpRow(rowOut);
                manifestDflat.write(rowOut);
                versionContent.addFileComponent(fileComponent.getIdentifier(), fileComponent);
                outCnt++;
            }
            manifestDflat.writeEOF();
            if (outCnt == 0) {
                throw new TException.REQUEST_INVALID(
                        MESSAGE + "No files added from manifest - one is required");
            }
            return tempVersion;


        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);

        } finally {
            if (manifestDflat != null) {
                try {
                    manifestDflat.closeOutput();
                } catch (Exception ex) {}
            }
        }
    }

    /**
     * Set mime type using Tika
     * @param component file component with componentFile set
     * @throws TException 
     */
    protected void setTika(FileComponent component, File testFile)
        throws TException
    {
        InputStream stream = null;
        try {
            if (testFile == null) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "file not found:" + component.getIdentifier());
            }
            stream = new FileInputStream(testFile);
            String fileID = component.getIdentifier();
            String mimeType = tika.getMimeType(stream, fileID);
            component.setMimeType(mimeType);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }
    
    /**
     * Build a POST manifest specifically for VersionLink handling
     * @param getFileURL base URL reference to specific files
     * @param versionContent content information about files
     * @return File containing POST manifest
     * @throws TException
     */
    protected File getPOSTManifest(
            String baseURL,
            Identifier objectID,
            int versionID,
            VersionContent versionContent)
        throws TException
    {
        Vector<String> list = new Vector<>(100);
        getPOSTListManifest(list, baseURL, objectID, versionID, versionContent);
        return getManifestFileFromList(list);
    }

    /**
     * Build a list containing a manifest entry for each line
     * @param getFileURL base URL reference to specific files
     * @param versionContent content information about files
     * @return File containing POST manifest
     * @throws TException
     */
    protected void getPOSTListManifest(
            Vector<String> list,
            String baseURLS,
            Identifier objectID,
            int versionID,
            VersionContent versionContent)
        throws TException
    {
        ManifestRowAbs.ManifestType manifestType = ManifestRowAbs.ManifestType.add;
        try {
            if (StringUtil.isEmpty(baseURLS)) {
                String msg = MESSAGE
                    + "getPOSTListManifest - base URL not provided";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }
            Vector<FileComponent> fileStates  = versionContent.getFileComponents();
            ManifestRowAdd rowOut
                    = (ManifestRowAdd)ManifestRowAbs.getManifestRow(manifestType, logger);

            for (FileComponent fileState : fileStates) {
                URL fileLink = null;
                URL baseURL = new URL(baseURLS);
                try {
                    fileLink = StoreUtil.buildContentURL(
                            null,
                            baseURL,
                            null,
                            objectID,
                            versionID,
                            fileState.getIdentifier());
                } catch (Exception ex) {
                    throw new TException.INVALID_DATA_FORMAT(MESSAGE
                            + "getPOSTListManifest"
                            + " - passed URL format invalid: baseURL=" + baseURL
                            + " - Exception:" + ex);
                }
                fileState.setURL(fileLink);
                rowOut.setFileComponent(fileState);
                if (DEBUG)
                    System.out.println("!!!!DflatManager:" + rowOut.getLine());
                list.add(rowOut.getLine());
            }

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    /**
     * From a list containing formatted line output create a manifest file
     * @param list each entry is a manifest line
     * @return POST type manifest file
     * @throws TException
     */
    protected File getManifestFileFromList(
            Vector<String> list)
        throws TException
    {
        ManifestRowAbs.ManifestType manifestType = ManifestRowAbs.ManifestType.add;
        try {
            if ((list == null) || (list.size() == 0)) {
                String msg = MESSAGE
                    + "getManifestFileFromList - base URL not provided";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }
            File linkManifest = FileUtil.getTempFile(null, null);
            Manifest manifestDflat = Manifest.getManifest(logger, manifestType);
            manifestDflat.openOutput(linkManifest);
            for (String fileState : list) {
                if (DEBUG)
                    System.out.println("!!!!DflatManager:" + list);
                manifestDflat.write(fileState + NL);
            }
            manifestDflat.writeEOF();
            manifestDflat.closeOutput();
            return linkManifest;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    /**
     * Add this file to a full directory
     * @param fullDirectory full directory to contain this file
     * @param fileState state information for file including remote extract
     * information
     * @throws Exception
     */
    protected void addRow(
            File fullDirectory,
            FileComponent fileState)
        throws Exception
    {
        try {
            URL fileURL = fileState.getURL();
            String fileName = fileState.getIdentifier();
            File rowFile = fileState.getComponentFile();
            File componentFile = getComponentFile(fullDirectory, fileName);
            if (DEBUG)
                    System.out.println(MESSAGE + "!!!!addRow: fileName=" + fileName
                            + " - componentFile=" + componentFile.getCanonicalPath()
                            + " - fileState=" + fileState.dump("addRow")
                            );
            long componentBeforeTime = DateUtil.getEpochUTCDate();
            if (rowFile != null) {
                InputStream inStream = new FileInputStream(rowFile);
                FileUtil.stream2File(inStream, componentFile);
                
            } else if (fileURL.toString().toLowerCase().startsWith("file:")) {
                copyFileURL(logger, fileURL, componentFile);
                
            } else {
                FileUtil.url2File(logger, fileURL, componentFile);
            }
            long componentAfterTime = DateUtil.getEpochUTCDate();
            contentAccumTime += (componentAfterTime - componentBeforeTime);
            
            if (verifyOnWrite) {
                doFileFixity(componentFile, fileState);
            }
            long writeValidateTime = DateUtil.getEpochUTCDate();
            validateAccumTime += (writeValidateTime - componentAfterTime);
            if (StringUtil.isEmpty(fileState.getMimeType())) {
                setTika(fileState, componentFile);
            } 
        } catch (Exception ex) {
            throw makeTException("Could not addRow.", ex);
        }
        
    }
    
    protected void copyFileURL(LoggerInf logger, URL fileURL, File componentFile)
        throws TException
    {
        try {
            File inFile = FileUtil.fileFromURL(fileURL);
            if (!inFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                        + "copyFileURL - fileURL path does not exist:" + inFile.getCanonicalPath());
            }
            InputStream inStream = new FileInputStream(inFile);
            FileUtil.stream2File(inStream, componentFile);
            
        } catch (Exception ex) {
            throw makeTException("Could not addRow.", ex);
        }
    }

    /**
     * Run fixity on this file
     * @param componentFile file to be tested
     * @param fileState state information for file including remote extract
     * information
     * @throws Exception
     */
    protected void doFileFixity(
            File componentFile,
            FileComponent fileState)
        throws Exception
    {
        MessageDigest messageDigest = fileState.getMessageDigest();
        if (NULL_MESSAGE_DIGEST_ALLOWED && (messageDigest == null)) {
            if (DEBUG) System.out.println("doFileFixity: no messageDigest provided");
            if (fileState.getSize() > 0) {
                if (fileState.getSize() != componentFile.length()) {
                    String msg = MESSAGE + "addRow - Fixity fails on only size."
                            + " - File size=" + componentFile.length() + NL
                            + " - In   size=" + fileState.getSize() + NL;
                    throw new TException.INVALID_DATA_FORMAT(msg);
                }
            } else {
                fileState.setSize(componentFile.length());
            }
            FixityTests fixity = new FixityTests(componentFile, "sha-256", logger);
            fileState.setFirstMessageDigest(fixity.getChecksum(), "sha-256");
            return;
        }
        MessageDigestType algorithm = messageDigest.getAlgorithm();
        FixityTests fixity = new FixityTests(componentFile, algorithm.toString(), logger);
        String checksum = fileState.getMessageDigest().getValue();
        long size = fileState.getSize();
        FixityTests.FixityResult fixityResult = fixity.validateSizeChecksum(checksum, algorithm.toString(), size);
        if (!fixityResult.fileSizeMatch || !fixityResult.checksumMatch) {
            String msg = MESSAGE + "addRow - Fixity fails."
                    + fixityResult.dump("");
            System.out.println("Fixity File:" + NL
                    + " - File checksum=" + fixity.getChecksum() + NL
                    + " - In   checksum=" + checksum + NL
                    + " - File size=" + componentFile.length() + NL
                    + " - In   size=" + size + NL
                    + " - File algorithm=" + algorithm.toString() + NL);
            throw new TException.INVALID_DATA_FORMAT(msg);
        }
    }

    /**
     *
     * @param fullDirectory
     * @param fileState
     * @throws Exception
     */
    protected void fixityRow(
            File fullDirectory,
            FileComponent fileState)
        throws Exception
    {
        try {
            String fileName = fileState.getIdentifier();
            File componentFile = getComponentFile(fullDirectory, fileName);
            doFileFixity(componentFile, fileState);

        } catch (Exception ex) {
            throw makeTException("Fixity fails for component.", ex);
        }

    }

    /**
     * Get a File that is the String fileName add to the directory.
     * The fileName may actually contain a directory path. the path is
     * is extracted and added as additional directories under fullDirectory
     * @param fullDirectory Directory to contain the fileName path
     * @param fileName name of file (with potential path) to be added under directory
     * @return directory + path + file
     */
    public static File getComponentFile(File fullDirectory, String fileName)
    {
        if (StringUtil.isEmpty(fileName)) return null;
        int i=0;
        for (i=(fileName.length()-1); i >= 0; i--) {
            char c = fileName.charAt(i);
            if ((c == '/') || (c == '\\')) {
                break;
            }
        }
        File componentFile = null;
        if (i > 0) {
            String componentPath = fileName.substring(0,i);
            String componentFileName = fileName.substring(i+1);
            File pathFile = new File(fullDirectory, componentPath);
            if (!pathFile.exists()) {
                pathFile.mkdirs();
            }
            componentFile = new File(pathFile, componentFileName);

        } else {
            componentFile = new File(fullDirectory, fileName);
        }
        return componentFile;
    }

    /**
     * create a full directory path (not content)
     * @param versionDirectory parent directory
     * @return full directory
     * @throws TException process exception
     */
    protected File createFullDirectory(
            File versionDirectory)
        throws TException
    {
        try {
            File versionFull = new File(versionDirectory, "full");
            if (!versionFull.exists()) versionFull.mkdir();
            return versionFull;

        } catch (Exception ex) {
            String msg = MESSAGE
                    + "createVersionDirectory - failed to create version directory."
                    + " Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }

    /**
     * Build dflat info file from local dflatInfo properties
     * @throws TException process Exception
     */
    protected void buildDflatInfoFile()
            throws TException
    {
        String txt = dflatInfo.getLoadProperties();
        try {
            File dflatInfoFile = new File(m_objectStoreBase, "dflat-info.txt");
            if (dflatInfoFile.exists()) return;
            FileUtil.string2File(dflatInfoFile, txt);

        } catch (Exception ex) {
            logger.logError(MESSAGE + "dflat-info.txt Exception:" + ex,  LoggerInf.LogLevel.SEVERE);
        }
    }

    /**
     * Build Dflat Namaste file
     * @throws TException process exception
     */
    protected void buildDflatNamasteFile()
            throws TException
    {
        buildNamasteFile(m_objectStoreBase);
    }

    protected void dumpRow(ManifestRowObject row)
    {
        FileComponent fileComponent = row.getFileComponent();
        String line = fileComponent.dump(MESSAGE);
        log(MESSAGE + "manifest row=" + line, 10);
    }

    protected void log(String msg, int lvl)
    {
        //System.out.println(msg);
        logger.logMessage(msg, lvl, true);
    }

    /**
     * Return a storage file
     * @param versionID version identifier of file to be returned
     * @param fileName name of file to be returned
     * @return storage file
     * @throws TException process exception
     */
    public File getFile(
            int versionID,
            String fileName)
        throws TException
    {
        int currentVersionID = testVersionCurrent(versionID);
        if (versionID < 1) versionID = currentVersionID;
        if (DEBUG) System.out.println("getFile - versionID:" + versionID + " - fileName:" + fileName);
        return m_dflatVersionManager.getVersionFile(versionID, fileName);
    }

    /**
     * Return a file after optionally performing a fixity test
     * @param versionID version identifier for fixity
     * @param fileName component file for fixity
     * @return fixity state information
     * @throws TException process exception
     */
    public File getFileFixity(
            int versionID,
            String fileName)
        throws TException
    {
        return getFileFixity(versionID, fileName, true);
    }

    /**
     * Return a file after optionally performing a fixity test
     * @param versionID version identifier for fixity
     * @param fileName component file for fixity
     * @return fixity state information
     * @throws TException process exception
     */
    public File getFileFixity(
            int versionID,
            String fileName,
            boolean verifyOnReadLocal)
        throws TException
    {
        try {
            int currentVersionID = testVersionCurrent(versionID);
            if (versionID < 1) versionID = currentVersionID;
            FileComponent fileState = getFileState(versionID, fileName);
            if (fileState == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Requested file not found:" + fileName);
            }
            File file = m_dflatVersionManager.getVersionFile(versionID, fileName);
            if ((file == null) || !file.exists()) {
                throw new TException.INVALID_ARCHITECTURE("Requested file in manifest but not found:" + fileName);
            }
            if (verifyOnReadLocal) {
                doFileFixity(file, fileState);
            }

            return file;

        } catch (Exception ex) {
            throw makeTException("File fixity fails.", ex);
        }
    }

    /**
     * Perform and retrieve fixity information for specific version and file
     * @param versionID version identifier for fixity
     * @param fileName component file for fixity
     * @return fixity state information
     * @throws TException process exception
     */
    public FileFixityState getFileFixityState(
            int versionID,
            String fileName)
        throws TException
    {
        try {
            int currentVersionID = testVersionCurrent(versionID);
            if (versionID < 1) versionID = currentVersionID;
            FileComponent fileState = getFileState(versionID, fileName);
            if (fileState == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Requested file not found:" + fileName);
            }
            File file = m_dflatVersionManager.getVersionFile(versionID, fileName);
            if ((file == null) || !file.exists()) {
                throw new TException.INVALID_ARCHITECTURE("Requested file in manifest but not found:" + fileName);
            }
            MessageDigest fileDigest = fileState.getMessageDigest();
            String checksumType = fileDigest.getJavaAlgorithm();
            FileFixityState fixityState = new FileFixityState();
            fixityState.setManifestDigest(fileDigest);
            fixityState.setManifestFileSize(fileState.getSize());

            FixityTests fixity = new FixityTests(file, checksumType, logger);
            fixityState.setFileSize(fixity.getInputSize());
            MessageDigest fixityDigest = new MessageDigest(fixity.getChecksum(), fixity.getChecksumType());
            fixityState.setFileDigest(fixityDigest);
            String checksum = fileState.getMessageDigest().getValue();
            long size = fileState.getSize();
            FixityTests.FixityResult fixityResult = fixity.validateSizeChecksum(checksum, checksumType, size);
            fixityState.setSizeMatches(fixityResult.fileSizeMatch);
            fixityState.setDigestMatches(fixityResult.checksumMatch);
            fixityState.setFixityDate(DateUtil.getCurrentDate());

            return fixityState;

        } catch (Exception ex) {
            throw makeTException("File fixity fails.", ex);
        }
    }

    /**
     * Return an archive for this object
     * @param objectID object identifier for extracting archive
     * @param returnFullVersion true=return all versions a full directories, false=return as saved
     * @param returnIfError do not perform fixity testing
     * @param archiveType type of archive to return: zip, tar, targz
     * @return archive and archive state info
     * @return
     * @throws TException
     */
    public FileContent getObject(
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        if (StringUtil.isEmpty(archiveTypeS)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not supplied");
        }
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilder.ArchiveType archiveType = null;
        try {
            archiveType
                    = ArchiveBuilder.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not recognized");
        }
        FileContent fileContent = null;
        if (returnFullVersion) {
            fileContent = getFullObject(objectID, returnIfError, archiveType);
        } else {
            fileContent = buildObjectArchive(m_objectStoreBase, archiveType);
        }
        return fileContent;
    }

    /**
     * Return an archive for this object
     * @param objectID object identifier for extracting archive
     * @param returnFullVersion true=return all versions a full directories, false=return as saved
     * @param returnIfError do not perform fixity testing
     * @param archiveType type of archive to return: zip, tar, targz
     * @return archive and archive state info
     * @return
     * @throws TException
     */
    public void getObjectStream(
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        System.out.println(MESSAGE + "getObjectStream entered");
        if (StringUtil.isEmpty(archiveTypeS)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not supplied");
        }
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilder.ArchiveType archiveType = null;
        try {
            if (DEBUG) System.out.println(MESSAGE + "getObjectStream before ArchiveBuilder");
            archiveType
                    = ArchiveBuilder.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not recognized");
        }
        FileContent fileContent = null;
        if (returnFullVersion) {
            getFullObject(objectID, returnIfError, archiveType, outputStream);
        } else {
            buildObjectArchive(m_objectStoreBase, archiveType, outputStream);
        }
    }

    /**
     * Return an archive containing full directories - no delta's
     * @param objectID object identifier for extracting archive
     * @param returnIfError do not perform fixity testing
     * @param archiveType type of archive to return: zip, tar, targz
     * @return archive and archive state info
     * @throws TException
     */
    public FileContent getFullObject(
            Identifier objectID,
            boolean returnIfError,
            ArchiveBuilder.ArchiveType archiveType)
        throws TException
    {
        File tempObject = null;
        try {
            tempObject = FileUtil.getTempDir("arc");
            String id = PairtreeUtil.getPairName(objectID.getValue());
            //id = id.replace('\\', '_');
            //id = id.replace('/', '_');
            if (DEBUG) System.out.println("***id=" + id);
            File objectFile = new File(tempObject, id);
            objectFile.mkdir();
            buildFullObject(objectFile, objectID, returnIfError);
            FileContent fileContent = buildObjectArchive(objectFile, archiveType);
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(null, ex);

        } finally {
            if (tempObject != null) {
                try {
                    FileUtil.deleteDir(tempObject);
                } catch (Exception ex) {}
            }
        }

    }

    /**
     * Return an archive containing full directories - no delta's
     * @param objectID object identifier for extracting archive
     * @param returnIfError do not perform fixity testing
     * @param archiveType type of archive to return: zip, tar, targz
     * @return archive and archive state info
     * @throws TException
     */
    public void getFullObject(
            Identifier objectID,
            boolean returnIfError,
            ArchiveBuilder.ArchiveType archiveType,
            OutputStream outputStream)
        throws TException
    {
        File tempObject = null;
        try {
            tempObject = FileUtil.getTempDir("arc");
            String id = PairtreeUtil.getPairName(objectID.getValue());
            //id = id.replace('\\', '_');
            //id = id.replace('/', '_');
            if (DEBUG) System.out.println("***id=" + id);
            File objectFile = new File(tempObject, id);
            objectFile.mkdir();
            buildFullObject(objectFile, objectID, returnIfError);
            buildObjectArchive(objectFile, archiveType, outputStream);

        } catch (Exception ex) {
            throw makeTException(null, ex);

        } finally {
            if (tempObject != null) {
                try {
                    FileUtil.deleteDir(tempObject);
                } catch (Exception ex) {}
            }
        }

    }

    /**
     * Run fixity and validation for this entire object
     * @param objectID object identifier of storage to be validated and fixity tested
     * @throws TException
     */
    public void fixityFullObject(
            Identifier objectID)
        throws TException
    {
        File tempObject = null;
        try {
            tempObject = FileUtil.getTempDir("arc");
            String id = objectID.getValue();
            id.replace('\\', '_');
            id.replace('/', '_');
            File objectFile = new File(tempObject, id);
            objectFile.mkdir();
            buildFullObject(objectFile, objectID, false);
            return;

        } catch (Exception ex) {
            throw makeTException(null, ex);

        } finally {
            if (tempObject != null) {
                try {
                    FileUtil.deleteDir(tempObject);
                } catch (Exception ex) {}
            }
        }

    }

    /**
     * build a dflat object containing only full directories
     * @param objectFile directory containing dflat
     * @param objectID object identifier for extracting archive
     * @param returnIfError do not perform fixity testing
     * @return archive and archive state info
     * @throws TException
     */
    public void buildFullObject(
            File objectFile,
            Identifier objectID,
            boolean returnIfError)
        throws TException
    {
        int currentVersionID = m_dflatVersionManager.getNextVersion() - 1;
        if (currentVersionID < 1) {
            throw new TException.INVALID_ARCHITECTURE(
                        "Object does not contain any version");
        }
        try {
            if ((objectFile == null) || !objectFile.exists()) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "buildFullObject null objectFile");
            }
            if ((m_objectStoreBase == null) || !m_objectStoreBase.exists()) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "buildFullObject null m_objectStoreBase");
            }
            if (false) System.out.println("***buildFullObject:"
                    + " - objectFile:" + objectFile.getCanonicalPath()
                    + " - m_objectStoreBase:" + m_objectStoreBase.getCanonicalPath()
                    );
            FileUtil.copyDirectory(
                    new File(m_objectStoreBase, "log"),
                    new File(objectFile, "log"));
            FileUtil.copyFile("current.txt", m_objectStoreBase, objectFile);
            FileUtil.copyFile("dflat-info.txt", m_objectStoreBase, objectFile);
            FileUtil.copyReg("0\\=.*", m_objectStoreBase, objectFile);
            if (!m_dflatVersionManager.saveFullVersions(objectFile)){
                throw new TException.GENERAL_EXCEPTION("saveFullVersions fails");
            }

            boolean fixity = verifyOnWrite && !returnIfError;
            int fixityValidatedCnt = 0;
            for (int iver = 1; iver <= currentVersionID; iver++) {
                String versionName = DflatVersionReddManager.getVersionName(iver);
                File versionFile = new File(objectFile, versionName);
                validateFullVersion(iver, versionFile);
                if (fixity) {
                    try {
                        fixityFullVersion(iver, versionFile);
                    } catch (TException tex) {
                        throw new TException.FIXITY_CHECK_FAILS("Fixity error for:"
                                + " - objectID:" + objectID.getValue()
                                + " - versionID:" + iver, tex);
                    }
                    fixityValidatedCnt++;
                }
            }
            if (DEBUG) {
                System.out.println("Versions validated:" + fixityValidatedCnt);
            }

        } catch (Exception ex) {
            throw makeTException(null, ex);

        }

    }

    /**
     * Get archive file for a particular version
     * @param versionID version identifier to be extracted
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive types: "tar", "targz", "zip"
     * @return archive File and file state
     * @throws TException
     */
    public FileContent getVersionArchive(
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        if (StringUtil.isEmpty(archiveTypeS)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Archive type not supplied");
        }
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilder.ArchiveType archiveType = null;
        versionID = testVersionCurrent(versionID);
        try {
            archiveType
                    = ArchiveBuilder.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Archive type not recognized");
        }
        File fullVersion = m_dflatVersionManager.getFullVersion(versionID);
        try {
            validateFullVersion(versionID, fullVersion);
            
        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
        log("returnIfError=" + returnIfError, 10);
        if (verifyOnRead && !returnIfError) {
            try {
                fixityFullVersion(versionID, fullVersion);
            } catch (Exception ex) {
                throw new TException.FIXITY_CHECK_FAILS("Fixity fails", ex);
            }
        }
        FileContent fileContent = getVersionArchive(fullVersion, archiveType);
        return fileContent;
    }

    /**
     * Get archive file for a particular version
     * @param versionID version identifier to be extracted
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive types: "tar", "targz", "zip"
     * @return archive File and file state
     * @throws TException
     */
    public void getVersionArchiveStream(
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException
    {
        if (StringUtil.isEmpty(archiveTypeS)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Archive type not supplied");
        }
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilder.ArchiveType archiveType = null;
        versionID = testVersionCurrent(versionID);
        try {
            archiveType
                    = ArchiveBuilder.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Archive type not recognized");
        }
        File fullVersion = m_dflatVersionManager.getFullVersion(versionID);
        try {
            validateFullVersion(versionID, fullVersion);
            
        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
        log("returnIfError=" + returnIfError, 10);
        if (verifyOnRead && !returnIfError) {
            try {
                fixityFullVersion(versionID, fullVersion);
            } catch (Exception ex) {
                throw new TException.FIXITY_CHECK_FAILS("Fixity fails", ex);
            }
        }
        getVersionArchiveStream(fullVersion, archiveType, outputStream);
    }




    /**
     * Return the full Version into a passed File
     * @param versionID version to return
     * @param fullVersion File to contain full Version response
     * @throws TException process exception
     */
    public void getFullVersion(
            int versionID,
            File fullVersion)
        throws TException
    {
        try {
            if (!m_dflatVersionManager.getFullVersion(versionID, fullVersion)) {
                throw new TException.GENERAL_EXCEPTION("getFullVersion-Unable to build fullVersion");
            }
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION(ex);
        }
        try {
            validateFullVersion(versionID, fullVersion);
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION(ex);
        }
        if (verifyOnWrite) {
            try {
                fixityFullVersion(versionID, fullVersion);
            } catch (Exception ex) {
                throw new TException.FIXITY_CHECK_FAILS("Fixity exception",ex);
            }
        }
    }

    /**
     * Build an addVersion manifest from an existing file
     * @param versionID version identifier for addVersion manifest
     * @param linkFileURL URL to use in constructing manifest
     * @return addVersion manifest as File (with File state)
     * @throws TException process exception
     */
    public FileContent getVersionLink(
            Identifier objectID,
            int versionID,
            String linkURL)
        throws TException
    {
        if (StringUtil.isEmpty(linkURL)) {
              throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - linkFileURL not supplied");
        }
        testVersionCurrent(versionID);
        VersionContent versionContent = null;
        try {
            versionContent = getVersionContentFromManifestFile(versionID);

        } catch (Exception ex) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Version not found:" + versionID);
        }
        File manifestFile = getPOSTManifest(linkURL, objectID, versionID, versionContent);
        FileContent fileContent = FileContent.getFileContent(manifestFile, logger);
        return fileContent;
    }

    /**
     * Build an addVersion manifest from an existing file
     * @param versionID version identifier for addVersion manifest
     * @param linkFileURL URL to use in constructing manifest
     * @return addVersion manifest as File (with File state)
     * @throws TException process exception
     */
    public FileContent getObjectLink(
            Identifier objectID,
            String linkURL)
        throws TException
    {
        if (StringUtil.isEmpty(linkURL)) {
              throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getVersionLink - linkFileURL not supplied");
        }
        VersionContent versionContent = null;
        int nextVersion = m_dflatVersionManager.getNextVersion();
        if (nextVersion == 1) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("No version found for this object");
        }
        int currentVersion = nextVersion--;
        Vector<String> list = new Vector<String>();
        try {
            for (int versionID=1; versionID < currentVersion; versionID++) {
                versionContent = getVersionContentFromManifestFile(versionID);
                getPOSTListManifest(list, linkURL, objectID, versionID, versionContent);
            }
            File manifestFile = getManifestFileFromList(list);
            FileContent fileContent = FileContent.getFileContent(manifestFile, logger);
            return fileContent;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "getObjectLink exception:" + ex;
            logger.logError(msg, 0);
            logger.logError(MESSAGE + "trace=" + StringUtil.stackTrace(ex), 10);
            throw new TException.REQUESTED_ITEM_NOT_FOUND(ex);
        }
    }

    /**
     * Get an archive file from a specific version
     * @param versionFile version directory file to return as full archive
     * @param archiveType type of archive (tar, targz, zip)
     * @return Archive file and state info of archive file
     * @throws TException process exception
     */
    public FileContent buildObjectArchive(
            File objectFile,
            ArchiveBuilder.ArchiveType archiveType)
        throws TException
    {
        File readme = null;
         try {
            if (objectFile == null) {
                throw new TException.INVALID_ARCHITECTURE(
                        "Object not found - unable to build content object");
            }
            File archiveFile = FileUtil.getTempFile("archive", "." + archiveType.getExtension());
            readme = addReadme(objectFile, "readmeObject.txt", "README.txt");
            ArchiveBuilder archiveBuilder
                    = ArchiveBuilder.getArchiveBuilder(
                        objectFile, archiveFile, logger, archiveType);
            FileContent archiveContent = setFileContent(archiveFile);
            return archiveContent;

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        } finally {
             try {
                 if (readme != null) {
                     readme.delete();
                     logger.logMessage("Delete README=" + readme.getAbsolutePath(), 10, true);
                 }
             } catch (Exception ex) { }
        }
    }

    /**
     * Get an archive file from a specific version
     * @param versionFile version directory file to return as full archive
     * @param archiveType type of archive (tar, targz, zip)
     * @return Archive file and state info of archive file
     * @throws TException process exception
     */
    public void buildObjectArchive(
            File objectFile,
            ArchiveBuilder.ArchiveType archiveType,
            OutputStream outputStream)
        throws TException
    {
        File readme = null;
        try {
            if (objectFile == null) {
                throw new TException.INVALID_ARCHITECTURE(
                        "Object not found - unable to build content object");
            }
            File archiveFile = FileUtil.getTempFile("archive", "." + archiveType.getExtension());
            readme = addReadme(objectFile, "readmeObject.txt", "README.txt");
            ArchiveBuilder archiveBuilder
                    = ArchiveBuilder.getArchiveBuilder(
                        objectFile, outputStream, logger, archiveType);
            

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        } finally {
             try {
                 if (readme != null) {
                     readme.delete();
                     logger.logMessage("Delete README=" + readme.getAbsolutePath(), 10, true);
                 }
             } catch (Exception ex) { }
        }
    }

    /**
     * Get an archive file from a specific version
     * @param versionFile version directory file to return as full archive
     * @param archiveType type of archive (tar, targz, zip)
     * @param outputStream http output stream
     * @throws TException process exception
     */
    public void getVersionArchiveStream(
            File versionFile,
            ArchiveBuilder.ArchiveType archiveType,
            OutputStream outputStream)
        throws TException
    {
        File readme = null;
        try {
            if (versionFile == null) {
                throw new TException.INVALID_ARCHITECTURE(
                        "Unable to build content version");
            }
            readme = addReadme(versionFile, "readmeVersion.txt", "README.txt");
            ArchiveBuilder archiveBuilder
                    = ArchiveBuilder.getArchiveBuilder(
                        versionFile, outputStream, logger, archiveType);

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        } finally {
             try {
                 if (readme != null) {
                     readme.delete();
                     logger.logMessage("Delete README=" + readme.getAbsolutePath(), 10, true);
                 }
             } catch (Exception ex) { }
        }
    }

    /**
     * Get an archive file from a specific version
     * @param versionFile version directory file to return as full archive
     * @param archiveType type of archive (tar, targz, zip)
     * @return Archive file and state info of archive file
     * @throws TException process exception
     */
    public FileContent getVersionArchive(
            File versionFile,
            ArchiveBuilder.ArchiveType archiveType)
        throws TException
    {
        File readme = null;
        try {
            if (versionFile == null) {
                throw new TException.INVALID_ARCHITECTURE(
                        "Unable to build content version");
            }
            File archiveFile = FileUtil.getTempFile("archive", "." + archiveType.getExtension());
            readme = addReadme(versionFile, "readmeVersion.txt", "README.txt");
            ArchiveBuilder archiveBuilder
                    = ArchiveBuilder.getArchiveBuilder(
                        versionFile, archiveFile, logger, archiveType);
            FileContent archiveContent = setFileContent(archiveFile);
            return archiveContent;

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        } finally {
             try {
                 if (readme != null) {
                     readme.delete();
                     logger.logMessage("Delete README=" + readme.getAbsolutePath(), 10, true);
                 }
             } catch (Exception ex) { }
        }
    }

    /**
     * Set file state information from input file including fixity info
     * @param file get state of this file
     * @return File and FileState info
     * @throws TException process exception
     */
    protected FileContent setFileContent(File file)
        throws TException
    {
        FileComponent fileComponent = new FileComponent();
        FixityTests fixityTests = new FixityTests(file, "SHA-256", logger);
        fileComponent.addMessageDigest(fixityTests.getChecksum(), fixityTests.getChecksumType().toString());
        fileComponent.setIdentifier(file.getName());
        fileComponent.setSize(file.length());
        fileComponent.setCreated();
        tika.setTika(fileComponent, file, file.getName());
        FileContent fileContent = FileContent.getFileContent(fileComponent, null, file);
        return fileContent;
    }

    /**
     * Get full versions into temp directory down through versionID
     * @param extractDir temp directory to contain full version
     * @param versionID build full versions down to this versionID
     * @return recover file containing full versions
     * @throws org.cdlib.mrt.utility.TException
     */
    protected File getVersions(File extractDir, int versionID)
        throws TException
    {
        try {

            if (!extractDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getVersion - extractDir not provided");
            }
            File recover = new File(extractDir, "recover");
            if (recover.exists()) {
                FileUtil.deleteDir(recover);
            }
            recover.mkdir();
            int nextVersion = m_dflatVersionManager.getNextVersion();
            if (nextVersion == 1) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE
                        + "No version found for this object");
            }
            File verFile = null;
            for (int ver=nextVersion-1; ver >= versionID; ver--) {
                String verName = DflatVersionManagerAbs.getVersionName(ver);
                verFile = new File(recover, verName);
                verFile.mkdir();
                m_dflatVersionManager.getFullVersion(ver, verFile);
                log("Created:" + verFile.getCanonicalPath(), 0);
                try {
                    validateFullVersion(ver, verFile);
                } catch (Exception ex) {
                    throw new TException.GENERAL_EXCEPTION(MESSAGE
                        + "getVersion - Exception:" + ex);
                }
                try {
                    fixityFullVersion(ver, verFile);
                } catch (Exception ex) {
                    throw new TException.FIXITY_CHECK_FAILS(MESSAGE
                        + "getVersion - fixity exception:" + ex);
                }
            }
            return verFile;

        }  catch(TException tex)  {
                logger.logError(MESSAGE + "Exception:" + tex, 0);
                logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(tex), 10);
                throw tex;

        }  catch(Exception ex)  {
                logger.logError(MESSAGE + "Exception:" + ex, 0);
                logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex), 10);
                throw new TException.GENERAL_EXCEPTION(MESSAGE
                        + "getVersion - Exception:" + ex);
        }
    }

    /**
     * Get the File Component for a specific file
     * @param versionID version identifier
     * @param fileName name of file to get state information
     * @return File component information
     * @throws TException process exception
     */
    public FileComponent getFileState(
            int versionID,
            String fileName)
        throws TException
    {
        int currentVersionID = testVersionCurrent(versionID);
        if (versionID < 1) versionID = currentVersionID;
        VersionContent versionContent
                = getVersionContentFromManifestFile(versionID);
        if (versionContent == null) return null;
        for (FileComponent fileComponent : versionContent.getFileComponents()) {
            if (fileComponent.getIdentifier().equals(fileName)) {
                if (DEBUG) System.out.println("***FileComponent - MimeType=" + fileComponent.getMimeType());
                return fileComponent;
            }
        }
        return null;
    }

    /**
     * Attempt to get a lock file class. This method will wait for 3 minutes attemptin
     * to get the lock if not immediately available
     * @return lock class
     * @throws TException process exception
     */
    protected TLockFile getDflatLockException()
            throws TException
    {
        TLockFile fileLock = null;
        try {
            fileLock =
                TLockFile.getReplaceTLockFile(
                        m_objectStoreBase,
                        "lock.txt",
                        10800, 10800, 3, 3);
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
            throw new TException.GENERAL_EXCEPTION( err);

        }
    }

    /**
     * Remove this lock file
     * @param fileLock lock file
     */
    protected void removeLock(TLockFile fileLock)
    {
        if (fileLock == null) return;
        try {
            fileLock.remove();

        } catch (TException fe) {
            return;

        } catch(Exception ex) {
            return;

        }
    }


    /**
     * Add date LastAddVersion date property to last-activity.txt
     * @throws TException process exception
     */
    protected void setLogAddVersion()
            throws TException
    {
        try {
            LastActivity lastActivity = getLastActivityFile();
            lastActivity.setPropery(LastActivity.Type.LASTADDVERSION);
            lastActivity.writeFile();

        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
    }

    /**
     * Add LastFixity date property to last-activity.txt
     * @throws TException process exception
     */
    protected void setLogFixity()
            throws TException
    {
        try {
            LastActivity lastActivity = getLastActivityFile();
            lastActivity.setPropery(LastActivity.Type.LASTFIXITY);
            lastActivity.writeFile();

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
    }

    /**
     * Add date LastVersionDelete date property to last-activity.txt
     * @throws TException process exception
     */
    protected void setLogVersionDelete()
            throws TException
    {
        try {
            LastActivity lastActivity = getLastActivityFile();
            lastActivity.setPropery(LastActivity.Type.LASTDELETEVERSION);
            lastActivity.writeFile();

        } catch (Exception ex) {
            throw makeTException(null, ex);
        
        }
    }

    /**
     * Return LastActivity class - if last-activity file does not exist
     * then create it and intermediate directory
     * @throws TException process exception
     */
    protected LastActivity getLastActivityFile()
            throws TException
    {
        try {
            File log = new File(m_objectStoreBase, "log");
            if (!log.exists()) {
                log.mkdir();
            }
            File lastActivityFile = new File(log, "last-activity.txt");
            return new LastActivity(lastActivityFile);

        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
    }

    /**
     * Handle a remote exception. If the exception is a TException
     * then do not recreate it
     * @param header header to use in exception
     * @param ex remote exception
     * @return TException to be thrown
     */
    protected TException makeTException(String header, Exception ex)
    {
        TException tex = null;
        if (ex instanceof TException) {
            tex = (TException)ex;
        } else {
            tex = new TException.GENERAL_EXCEPTION(header, ex);
        }
        logger.logError(tex.toString(), 0);
        logger.logError(tex.dump(NAME), 10);
        return tex;
    }

    /**
     * get manifest File for this version
     * @param versionID version identifier for the manifest File
     * @return manifest.txt file
     */
    public File getVersionManifest(int versionID)
    {
        try {

            int currentVersionID = testVersionCurrent(versionID);
            if (versionID < 1) versionID = currentVersionID;
            String versionName = m_dflatVersionManager.getVersionName(versionID);
            File versionFile = new File(m_objectStoreBase,versionName);
            if ((versionFile == null) || !versionFile.exists()) return null;
            File manifestFile = new File(versionFile, MANIFESTTXT);
            if (!manifestFile.exists()) return null;
            return manifestFile;

        } catch (Exception ex) {
            return null;
        }
    }
    
    
    /**
     * Get list of file components for a version
     * @param versionID version identifier for the manifest File
     * @return list file components
     * @throws TException 
     */
    public Vector<FileComponent> getFileComponents(int versionID)
        throws TException
    {
        int currentVersionID = testVersionCurrent(versionID);
        if (versionID < 1) versionID = currentVersionID;
        VersionContent versionContent
                = getVersionContentFromManifestFile(versionID);
        if (versionContent == null) return null;
        return versionContent.getFileComponents();
    }
    
    /**
     * Get cloud version map using dflat
     * @param objectID object identifier
     * @return cloud version map
     * @throws TException 
     */
    public VersionMap getVersionMap(
                Identifier objectID)
        throws TException
    {
        VersionMap versionMap = new VersionMap(objectID, logger);
        try {
              int nextVersionID = m_dflatVersionManager.getNextVersion();
              if (nextVersionID == 1) return versionMap;
              for (int iver=1; iver < nextVersionID; iver++) {
                  Vector<FileComponent> components = getFileComponents(iver);
                  setCloudKey(versionMap, objectID, iver, components);
                  versionMap.addVersion(components);
              }
              
              return versionMap;

        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
    }
    
    protected void setCloudKey(
                VersionMap versionMap,
                Identifier objectID,
                int versionID,
                List<FileComponent> components)
        throws TException
    {
        try {
            for (FileComponent component : components) {
                String key = CloudUtil.getKey(objectID, versionID, component.getIdentifier(), false);
                component.setLocalID(key);
                versionMap.setCloudComponent(component, false);
            }
            
        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
        
    }
    
    /**
     * Return cloud manifest.xml for this object
     * @param objectID object identifier
     * @param validate because dflat is generated from scratch not used
     * @return manifest.xml
     * @throws TException 
     */
    public FileContent getCloudManifest(
            Identifier objectID,
            boolean validate)
        throws TException
    {
        try {
            VersionMap versionMap = getVersionMap(objectID);
            File manifestFile = FileUtil.getTempFile("man", ".xml");
            FileOutputStream  outStream = new FileOutputStream(manifestFile);
            ManifestStr.buildManifest(versionMap, outStream);
            return FileContent.getFileContent(manifestFile, logger);
            
        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
    }
    
    /**
     * Return cloud manifest.xml for this object
     * @param objectID object identifier
     * @param validate because dflat is generated from scratch not used
     * @param  outStream manifest.xml
     * @throws TException 
     */
    public void getCloudManifestStream(
            Identifier objectID,
            boolean validate,
            OutputStream outStream)
        throws TException
    {
        try {
            VersionMap versionMap = getVersionMap(objectID);
            ManifestStr.buildManifest(versionMap, outStream);
            
        } catch (Exception ex) {
            throw makeTException(null, ex);
        }
    }

    /**
     * Validate versionID against current - zero or less versionID is current
     * @param versionID versionID tested against current
     * @return current versionID
     * @throws org.cdlib.mrt.utility.MException
     */
    public int testVersionCurrent(int versionID)
        throws TException
    {
        boolean versionExists = false;
        int currentVersionID = m_dflatVersionManager.getNextVersion() - 1;
        if (currentVersionID >= 1) {
            if (versionID < 1) versionID = currentVersionID;
            if (versionID <= currentVersionID) versionExists = true;
            else versionExists = false;
        } else versionExists = false;
        if (!versionExists) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        "Object does not contain this version: " + versionID);
        }
        return versionID;
    }

    /**
     * Return locally saved SpecScheme for Dflat
     * @return dflat SpecScheme
     */
    public SpecScheme getNamasteSpec()
    {
        return namasteSpec;
    }

    /**
     * create a namast file using a SpecScheme
     * @param namasteFile file to be built
     * @throws TException process exception
     */
    protected void buildNamasteFile(File namasteFile)
        throws TException
    {
        this.namasteSpec.buildNamasteFile(namasteFile);
    }
    
    public File addReadme(
        File baseDirectory, String resourceName, String outfileName)
    throws TException
    {
         try {
            
            InputStream readmeStream =  this.getClass().getClassLoader().
                getResourceAsStream("resources/" + resourceName);
            if (readmeStream == null) return null;
            File readme = new File(baseDirectory, outfileName);
            FileUtil.stream2File(readmeStream, readme);
            return readme;
            
        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        }
    }

}





