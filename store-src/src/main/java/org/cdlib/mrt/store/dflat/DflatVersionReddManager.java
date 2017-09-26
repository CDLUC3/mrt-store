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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowObject;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.VersionContent;
/**
 * This class specifically handles Redd versioning within the context
 * of dflat. It is not generic to a non-dflat environment
 * @author dloy
 */
public class DflatVersionReddManager
        extends DflatVersionManagerAbs
{
    protected static final String NAME = "DflatVersionReddManager";
    protected static final String MESSAGE = NAME + ": ";

    public static final boolean DEBUG = false;
    public static final String DELETE_TXT = "delete.txt";
    public static final String NO_CHANGE_TXT = "no-change.txt";
    public static final String ADD = "add";
    public static final String DELTA_ADD = DELTA + "/" + ADD;
    protected final static String NL = System.getProperty("line.separator");

    public DflatVersionReddManager(File objectStoreBase, LoggerInf logger, DflatInfo dflatInfo)
        throws TException
    {
        super(objectStoreBase, logger, dflatInfo);
    }

    /**
     * Create delta for previous version and flip file versions
     * @param tempVersion
     * @throws org.cdlib.mrt.utility.MException
     */
    public void setCurrent(File tempVersion)
            throws TException
    {
        int currentVersionID = getNextVersion();
        int previousVersionID = currentVersionID - 1;
        File previousVersion = null;
        File previousDelta = null;
        File previousFull =  null;
        File currentVersion = null;
        try {
            currentVersion = createVersionDirectory(currentVersionID);
            String currentVersionName = currentVersion.getName();
            if (currentVersionID > 1) {
                previousVersion = getVersionFile(previousVersionID);
                previousDelta = new File(m_objectStoreBase, "previousDelta-" + previousVersionID);
                previousDelta.mkdir();
                buildDeltaPrevious(previousDelta, tempVersion, previousVersion);
                previousFull =  new File(m_objectStoreBase, "previousFull-" + previousVersionID);
                if (previousFull.exists()) {
                    FileUtil.deleteDir(previousFull);
                }
            }
            tempVersion.renameTo(currentVersion);
            writeCurrentTxt(currentVersionName);
            if (currentVersionID > 1) {
                previousVersion.renameTo(previousFull);
                previousDelta.renameTo(previousVersion);
            }
            if (previousFull != null) {
                FileUtil.deleteDir(previousFull);
            }

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err , 3);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    /**
     * Use a full version of previous version to replace current
     * i.e. delete current version
     * @param currentVersionID before replace current version
     * @param previousFullVersion new full previous version
     * @throws TException process exception
     */
    public void replaceCurrentWithPreviousOriginal(int currentVersionID, File previousFullVersion)
        throws TException
    {
        int previousVersionID = currentVersionID - 1;
        logger.logMessage(MESSAGE + "replaceCurrentWithPrevious "
                + " - currentVersionID=" + currentVersionID
                + " - previousVersionID=" + previousVersionID
                , 10);
        String currentName = getVersionName(currentVersionID);
        String previousName = getVersionName(previousVersionID);
        try {
            File previousDeltaVersion = getVersionNameFile(previousVersionID);
            if (!previousDeltaVersion.exists()) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE 
                        + "previous version does not exist:" + previousVersionID);
            }
            File deletePreviousDeltaVersion =  new File(m_objectStoreBase, "deleteDelta-" + previousName);
            if (deletePreviousDeltaVersion.exists()) FileUtil.deleteDir(deletePreviousDeltaVersion);
            previousDeltaVersion.renameTo(deletePreviousDeltaVersion);
            
            File currentFullVersion = getVersionNameFile(currentVersionID);
            if (currentFullVersion == null) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE 
                        + "current version does not exist:" + currentVersionID);
            }
            File deleteCurrentFullVersion = new File(m_objectStoreBase, "deleteCurrent-" + currentName);
            if (deleteCurrentFullVersion.exists())  FileUtil.deleteDir(deleteCurrentFullVersion);
            currentFullVersion.renameTo(deleteCurrentFullVersion);
            
            String previousFullName 
                    = getVersionName(previousVersionID);
            File newCurrentFull = new File(m_objectStoreBase, previousFullName);
            if (newCurrentFull.exists()) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE 
                        + "new current version does not exist:" + previousVersionID);
            }
            previousFullVersion.renameTo(newCurrentFull);

            writeCurrentTxt(previousFullName);
            FileUtil.deleteDir(deletePreviousDeltaVersion);
            FileUtil.deleteDir(deleteCurrentFullVersion);

        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String err = "replaceCurrentWithPrevious - Exception:" + ex;
            logger.logError(err , 3);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.INVALID_ARCHITECTURE(MESSAGE + err);
        }
    }

    /**
     * Use a full version of previous version to replace current
     * i.e. delete current version
     * @param currentVersionID before replace current version
     * @param previousFullVersion new full previous version
     * @throws TException process exception
     */
    public void replaceCurrentWithPrevious(int currentVersionID, File previousFullVersion)
        throws TException
    {
        int previousVersionID = currentVersionID - 1;
        logger.logMessage(MESSAGE + "replaceCurrentWithPrevious "
                + " - currentVersionID=" + currentVersionID
                + " - previousVersionID=" + previousVersionID
                , 10);
        String currentName = getVersionName(currentVersionID);
        String previousName = getVersionName(previousVersionID);
        try {
            File previousDeltaVersion = getVersionNameFile(previousVersionID);
            if (previousDeltaVersion == null) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE
                        + "previous version does not exist:" + previousVersionID);
            }
            File deletePreviousDeltaVersion =  new File(m_objectStoreBase, "deleteDelta-" + previousName);
            if (deletePreviousDeltaVersion.exists()) FileUtil.deleteDir(deletePreviousDeltaVersion);

            File currentFullVersion = getVersionNameFile(currentVersionID);
            if (currentFullVersion == null) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE
                        + "current version does not exist:" + currentVersionID);
            }
            File deleteCurrentFullVersion = new File(m_objectStoreBase, "deleteCurrent-" + currentName);
            if (deleteCurrentFullVersion.exists())  FileUtil.deleteDir(deleteCurrentFullVersion);

            File newCurrentFullVersion = new File(m_objectStoreBase, previousName);
            previousDeltaVersion.renameTo(deletePreviousDeltaVersion);
            currentFullVersion.renameTo(deleteCurrentFullVersion);
            previousFullVersion.renameTo(newCurrentFullVersion);

            writeCurrentTxt(previousName);
            FileUtil.deleteDir(deletePreviousDeltaVersion);
            FileUtil.deleteDir(deleteCurrentFullVersion);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            String err = "replaceCurrentWithPrevious - Exception:" + ex;
            logger.logError(err , 3);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.INVALID_ARCHITECTURE(MESSAGE + err);
        }
    }

    /**
     * Return a file for versionID and fileName - if the file physically
     * exists within this version. Because of delta-ing this file may only exist
     * in a later version
     * @param versionID version identifier
     * @param fileName file name
     * @return requested file if exists within this full or delta
     * @throws TException
     */
    protected File getThisFile(int versionID, String fileName)
            throws TException
    {
        try {
            File versionFile = getVersionFile(versionID);
            File thisFile = new File(versionFile, FULL + "/" + fileName);
            if (DEBUG) System.out.println("getThisFile - full:" + thisFile.getCanonicalPath());
            if (thisFile.exists()) return thisFile;

            thisFile = new File(versionFile, DELTA_ADD + "/" + fileName);
            if (DEBUG) System.out.println("getThisFile - delta:" + thisFile.getCanonicalPath());
            if (thisFile.exists()) return thisFile;
            return null;

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "getNextVersion - unable to find version. Exception:" + ex);
        }
    }

    /**
     * Return storage file (vnnn - not copy) for this ID
     * @param versionID version identifier for the returned File
     * @return file corresponding to versionID
     */
    public File getVersionNameFile(int versionID)
    {

        try {
            int nextVersionID = getNextVersion();
            if (nextVersionID == 1) return null; // no versions
            int currentVersionID = nextVersionID--;
            if (versionID <= 0) versionID = currentVersionID;
            //System.out.println("!!!!" + MESSAGE + "versionID=" + versionID);
            String versionName = getVersionName(versionID);
            File versionFile = new File(m_objectStoreBase,versionName);
            //System.out.println("!!!!" + MESSAGE + "versionFile=" + versionFile.getCanonicalPath());
            if ((versionFile == null) || !versionFile.exists()) return null;
            return  versionFile;

        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Get this file based on versionID and fileID
     * Because of delta-ing the first file matching this versionID and
     * fileID from the delta moving forward will be the correct file.
     * @param versionID version identifier of requested file
     * @param fileID file identifier of requested file
     * @return File matching this request
     * @throws TException process exception
     */
    public File getVersionFile(int versionID, String fileID)
        throws TException
    {
        try {
            int nextVersionID = getNextVersion();
            if (nextVersionID == 1) return null; // no versions
            int currentVersionID = nextVersionID--;
            if (versionID <= 0) versionID = currentVersionID;
            if (versionID > currentVersionID) return null; // version not a valid range

            File versionFile = getVersionFile(versionID);
            if (versionFile == null) {
                if (DEBUG) System.out.println("getVersionFile - getVersionFile(versionID) is null");
                return null;
            }
            for (int thisVersionID=versionID; thisVersionID <= currentVersionID; thisVersionID++) {
                File thisFile = getThisFile(thisVersionID, fileID);
                if (thisFile != null) return thisFile;
            }
            if (DEBUG) System.out.println("getVersionFile - no match return null");
            return null;

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "getNextVersion - unable to find version. Exception:" + ex);
        }
    }

    /**
     * get manifest File for this version
     * @param versionID version identifier for the manifest File
     * @return manifest.txt file
     */
    public File getVersionManifest(int versionID)
    {
        try {
            String versionName = getVersionName(versionID);
            File versionFile = new File(m_objectStoreBase,versionName);
            if ((versionFile == null) || !versionFile.exists()) return null;
            File manifestFile = new File(versionFile, MANIFEST_TXT);
            if (!manifestFile.exists()) return null;
            return manifestFile;

        } catch (Exception ex) {
            return null;
        }
    }


    /**
     * Build a full version directory for a specific version
     * @param versionID identifier of version to be reconstructed
     * @param fullVersion user supplied file to contain reconstructed version
     * @return true-file reconstructed; false-file not reconstructed
     * @throws org.cdlib.mrt.utility.MException
     */
    public boolean getFullVersion(int versionID, File fullVersion)
        throws TException
    {
        try {
            int nextVersionID = getNextVersion();
            int lastVersionID = nextVersionID - 1;
            if (lastVersionID == 0) return false;

            if (fullVersion == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFullVersion - fullVersion not defined");
            }
            if (!fullVersion.exists()) fullVersion.mkdir();
            File nextVersion = null;
            for (int i=lastVersionID; i >= versionID; i--) {
                nextVersion = getVersionFile(i);
                buildFullVersion(fullVersion, nextVersion);
            }
            return true;

        } catch (TException mex) {
            throw mex;

        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "buildFullPrevious", ex);
        }

    }

    /**
     * Create full forms of all versions in a directory
     * @param objectFile directory to contain all version files
     * @return true=able to build directory, false=fails
     * @throws TException process exception
     */
    public boolean saveFullVersions(File objectFile)
        throws TException
    {
        File fullVersion = null;
        try {
            File nextVersion = null;
            int nextVersionID = getNextVersion();
            int lastVersionID = nextVersionID - 1;
            if (lastVersionID == 0) return false;

            if (objectFile == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getFullVersion - fullVersion not defined");
            }
            File storageFullVersion = getVersionNameFile(lastVersionID);
            fullVersion = new File(objectFile, "tempFull");
            FileUtil.copyDirectory(storageFullVersion, fullVersion);
            if (fullVersion == null) {
                throw new TException.INVALID_ARCHITECTURE("Current version not found:" + lastVersionID);
            }
            for (int iver=lastVersionID; iver >= 1; iver--) {
                File tempLast = new File(objectFile, getVersionName(iver));
                FileUtil.copyDirectory(fullVersion, tempLast);
                if (iver == 1) break;
                nextVersion = getVersionFile(iver - 1);
                buildFullVersion(fullVersion, nextVersion);
            }
            return true;

        } catch (TException mex) {
            throw mex;

        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "saveFullVersions", ex);
            
        } finally {
            if (fullVersion != null) {
                try {
                    FileUtil.deleteDir(fullVersion);
                } catch (Exception ex) {}
            }
        }
    }

    /**
     * Get full versions into temp directory down through versionID
     * @param versionID build full versions down to this versionID
     * @return temporary file containing full version
     * @throws org.cdlib.mrt.utility.TException
     */
    public File getFullVersion(int versionID)
        throws TException
    {
        try {
            File tempFile = FileUtil.getTempDir("aaaa", logger);
            String name = getVersionName(versionID);
            File recover = new File(tempFile, name);
            //File recover = new File(m_objectStoreBase, "recover");
            //File recover = new File(extractDir, "recover");
            if (recover.exists()) {
                FileUtil.deleteDir(recover);
            }
            recover.mkdir();
            getFullVersion(versionID, recover);
            return recover;

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
     * Reconstruct a full version from deltas
     * @param buildVersion
     * @param nextVersion - next version to be used for reconstruction
     * @throws org.cdlib.mrt.utility.MException
     */
    public void buildFullVersion(
            File buildVersion,
            File nextVersion)
        throws TException
    {
        try {
            if ((buildVersion == null) || !buildVersion.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "buildFullPrevious - current file not defined");
            }
            if ((nextVersion == null) || !nextVersion.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "buildFullPrevious - previous file not defined");
            }
            File buildFull = new File(buildVersion, FULL);
            if (!buildFull.exists()) {
                buildFull.mkdir();
            }
            FileUtil.copyFile(MANIFEST_TXT, nextVersion, buildVersion);
            File nextFull = new File(nextVersion, FULL);
            if (nextFull.exists()) {
                FileUtil.deleteDir(buildFull);
                FileUtil.copyDirectory(nextFull, buildFull);
                return;
            }
            File previousRedd = new File(nextVersion, DELTA);
            if (!previousRedd.exists()) {
                throw new TException.INVALID_ARCHITECTURE(
                        "Redd directory not found but required");
            }
            File nextNoChange = new File(previousRedd, NO_CHANGE_TXT);
            if (nextNoChange.exists()) {
                return;
            }
            File nextAdd = new File(previousRedd, ADD);
            if (nextAdd.exists()) {
                FileUtil.copyDirectory(nextAdd, buildFull);
            }
            File nextReddDelete = new File(previousRedd, DELETE_TXT);
            if (nextReddDelete.exists()) {
                deleteList(buildFull, nextReddDelete);
            }

        } catch (TException mex) {
            throw mex;

        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "buildFullPrevious", ex);
        }
    }



    /**
     * Build a full for the previous version
     * @param newPrevious constructed previous version containing a full directory
     * @param current version with full directory
     * @param previous version with delta directory
     */
    public void buildFullPrevious(
            File tempPrevious,
            File current,
            File previous)
        throws TException
    {
        try {
            initBuildPrevious(tempPrevious, current, previous);
            FileUtil.copyFile(MANIFEST_TXT, previous, tempPrevious);
            File currentFull = new File(current, FULL);
            File tempFull = new File(tempPrevious, FULL);
            if (!tempFull.exists()) tempFull.mkdir();
            File previousFull = new File(previous, FULL);
            if (previousFull.exists()) {
                FileUtil.copyDirectory(previousFull, tempFull);
                return;
            }
            File previousRedd = new File(previous, DELTA);
            if (!previousRedd.exists()) {
                throw new TException.INVALID_ARCHITECTURE(
                        "Redd directory not found but required");
            }
            File nextNoChange = new File(previousRedd, NO_CHANGE_TXT);
            if (nextNoChange.exists()) {
                return;
            }
            File previousAdd = new File(previousRedd, ADD);
            FileUtil.copyDirectory(currentFull, tempFull);
            if (previousAdd.exists()) {
                FileUtil.copyDirectory(previousAdd, tempFull);
            }
            File previousReddDelete = new File(previousRedd, DELETE_TXT);
            if (previousReddDelete.exists()) {
                deleteList(tempFull, previousReddDelete);
            }

        } catch (TException mex) {
            throw mex;

        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "buildFullPrevious", ex);
        }


    }

    /**
     * Build a Redd delta directory based on a new current and previous
     * full directory
     * @param deltaPrevious new delta directory to build
     * @param current current full directory
     * @param previous previous full directory
     */
    public void buildDeltaPrevious(
            File deltaPrevious,
            File current,
            File previous)
        throws TException
    {
        PrintStream deletePrintStream = null;
        try {
            initBuildPrevious(deltaPrevious, current, previous);

            // empty any temp version that has content
            if (deltaPrevious.isDirectory()) {
                String path = deltaPrevious.getAbsolutePath();
                FileUtil.deleteDir(deltaPrevious);
                deltaPrevious = new File(path);
                deltaPrevious.mkdir();
            }

            log("buildDeltaPrevious - deltaPrevious=" + deltaPrevious.getCanonicalPath(), 0);
            log("buildDeltaPrevious - current=" + current.getCanonicalPath(), 0);
            log("buildDeltaPrevious - previous=" + previous.getCanonicalPath(), 0);
            FileUtil.copyFile(MANIFEST_TXT, previous, deltaPrevious);
            File previousFull = new File(previous, FULL);
            if (!previousFull.exists()) {
                throw new TException.INVALID_ARCHITECTURE (
                        MESSAGE + "buildDeltaPrevious - previousFull must contain a full directory");
            }
            
            File deltaRedd = new File(deltaPrevious, DELTA);
            deltaRedd.mkdir();
            buildNamasteFile(deltaRedd);
            File deltaAdd = new File(deltaRedd, ADD);
            deltaAdd.mkdir();
            File deleteTxt = new File(deltaRedd, DELETE_TXT);
            FileOutputStream deleteStream = new FileOutputStream(deleteTxt);
            deletePrintStream = new PrintStream(deleteStream, true, "utf-8");
            
            VersionContent currentContent = getVersionContent(current);
            VersionContent previousContent = getVersionContent(previous);

            List<FileComponent> currentList = currentContent.getFileComponents();
            List<FileComponent> previousList = previousContent.getFileComponents();
            setContentFiles(previous, previousList);

            int addCnt = 0;
            int deleteCnt = 0;

            // build delete list
            for (FileComponent currentFileState : currentList) {
                String fileID = currentFileState.getIdentifier();
                FileComponent previousFileState = previousContent.getFileComponent(fileID);

                if (previousFileState == null) {
                    deleteCnt++;
                    deletePrintStream.print(fileID + LS);
                }
            }
            deletePrintStream.close();

            ManifestRowObject.ManifestType manifestType = ManifestRowObject.ManifestType.object;
            ManifestRowObject rowOut
                    = (ManifestRowObject)ManifestRowAbs.getManifestRow(manifestType, logger);
            Manifest manifestRedd = Manifest.getManifest(logger, manifestType);
            File manifestReddFile = new File(deltaPrevious, DMANIFEST);
            manifestRedd.openOutput(manifestReddFile);

            // build add directory
            for (FileComponent previousFileState : previousList) {
                String fileID = previousFileState.getIdentifier();
                FileComponent currentFileState = currentContent.getFileComponent(fileID);

                boolean match = matchFileStates(previousFileState, currentFileState);
                if (!match) {
                    FileUtil.copyFile(fileID, previousFull, deltaAdd);
                    String name = previousFileState.getIdentifier();
                    name = "add/" + name;
                    previousFileState.setIdentifier(name);
                    rowOut.setFileComponent(previousFileState);
                    manifestRedd.write(rowOut);
                    addCnt++;
                }
            }
            manifestRedd.writeEOF();
            manifestRedd.closeOutput();

            if ((addCnt == 0) && (deleteCnt == 0)) {
                File noChangeTxt = new File(deltaRedd, NO_CHANGE_TXT);
                FileUtil.string2File(noChangeTxt, "no-change" + NL);
            }

            if (deleteCnt == 0) {
                deleteTxt.delete();
            }
            
            if (addCnt == 0) {
                FileUtil.deleteDir(deltaAdd);
                manifestReddFile.delete();
            }

        } catch (TException mex) {
            throw mex;

        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "buildFullPrevious", ex);

        } finally {
            if (deletePrintStream != null) {
                try {
                    deletePrintStream.close();

                } catch (Exception ex) { }
            }
        }

    }

    /**
     * Insert the compent File into each FileComponent of a list
     * @param previousFullVersion File pointer to full version form in dflat
     * @param previousList list of file component to have File pointer inserted
     * @throws TException
     */
    public void setContentFiles(
            File previousFullVersion,
            List<FileComponent> previousList)
        throws TException
    {
        try {
            File previousFull = new File(previousFullVersion, "full");
            if (!previousFull.exists()) {
                throw new TException.INVALID_ARCHITECTURE("full file not found:" + previousFull.getCanonicalPath());
            }
            String fileID = null;

            for (FileComponent previousFileState : previousList) {
                fileID = previousFileState.getIdentifier();
                File localFile = new File(previousFull, fileID);
                if (!localFile.exists()) {
                    throw new TException.INVALID_ARCHITECTURE("full file not found:" + previousFull.getCanonicalPath());
                }
                previousFileState.setComponentFile(localFile);
            }

        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "buildFullPrevious", ex);

        }
    }

    /**
     * Use Redd delete.txt to delete files as part of  rebuild a full
     * directory
     * @param pruneDirectory directory base for deleteion
     * @param deleteFile delete.txt file containing line oriented list of files to be deleted
     * @throws TException process exception
     */
    protected void deleteList(
            File pruneDirectory,
            File deleteFile)
        throws TException
    {
        BufferedReader br = null;
        try {
            FileInputStream pruneStream = new FileInputStream(deleteFile);
            br = new BufferedReader(new InputStreamReader(pruneStream, "utf-8"));
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    br.close();
                    break;
                }
                File removeFile = new File(pruneDirectory, line);
                if (removeFile.exists()) {
                    removeFile.delete();
                }
            }


        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "copyList", ex);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ex) {}

            }
        }
    }

    /**
     * Write out current.txt using versionName
     * @param versionName version value of current.txt
     * @throws TException process exception
     */
    protected void writeCurrentTxt(String versionName)
        throws TException
    {
        try {
            File currentTxtFile = new File(m_objectStoreBase, "current.txt");
            FileUtil.string2File(currentTxtFile, versionName + NL);
            logger.logMessage(MESSAGE + "writeCurrentTxt:" + versionName, 10);

        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception writing current.txt:" + ex);
        }

    }

}
