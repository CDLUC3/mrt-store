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
import java.io.InputStream;

import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.MessageDigestType;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.store.VersionContent;
/**
 *
 * @author dloy
 */
public abstract class DflatVersionManagerAbs {
    protected static final String NAME = "DflatVersionManagerAbs";
    protected static final String MESSAGE = NAME + ": ";
    
    public static final String LS =  System.getProperty("line.separator");
    public static final String MANIFEST_TXT = "manifest.txt";
    public static final String FULL = "full";
    public static final String DELTA = "delta";
    public static final String DMANIFEST = "d-manifest.txt";

    protected LoggerInf logger = null;
    protected File m_objectStoreBase = null;
    protected DflatInfo dflatInfo = null;
    protected SpecScheme namasteSpec = null;

    /**
     * Factory: Redd Versioning manager
     * @param objectStoreBase directory file for building dflat leaf
     * @param logger process logger
     * @param dflatInfo Specification properties for this dflat
     * @return Redd versioning manager
     * @throws TException
     */
    public static DflatVersionReddManager getDflatVersionReddManager(
            File objectStoreBase,
            LoggerInf logger,
            DflatInfo dflatInfo)
            throws TException
    {
        DflatVersionReddManager reddVersionManager =
                new DflatVersionReddManager(objectStoreBase, logger, dflatInfo);
        return reddVersionManager;
    }

    /**
     * Constructor
     * @param objectStoreBase object store base
     * @param logger process logger
     * @param dflatInfo Specification properties for this dflat
     */
    public DflatVersionManagerAbs(
            File objectStoreBase,
            LoggerInf logger,
            DflatInfo dflatInfo)
        throws TException
    {
        this.logger = logger;
        m_objectStoreBase = objectStoreBase;
        this.dflatInfo = dflatInfo;
        namasteSpec = dflatInfo.getSpecScheme("delta", DflatInfo.DELTASCHEME);
    }

    /**
     * Build a full for the previous version
     * @param newPrevious constructed previous version containing a full directory
     * @param current version with full directory
     * @param previous version with delta directory
     */
    public abstract void buildFullPrevious(
            File newPrevious,
            File current,
            File previous)
        throws TException;

    /**
     * From the previous full version of a directory create a delta version
     * @param deltaPrevious new delta directory to create
     * @param current current full version directory
     * @param previous full directory of last version
     * @throws TException
     */
    public abstract void buildDeltaPrevious(
            File deltaPrevious,
            File current,
            File previous)
        throws TException;


    /**
     * Create delta for previous version and flip file versions
     * @param tempVersion
     * @throws org.cdlib.mrt.utility.MException
     */
    public abstract void setCurrent(File tempVersion)
            throws TException;

    /**
     * Return storage file (not copy) for this ID
     * @param versionID version identifier for the returned File
     * @return file corresponding to versionID
     */
    public abstract File getVersionNameFile(int versionID);

    /**
     * Delete the current version and replace with full version of previous
     * @param currentVersionID current version identifier
     * @param previousFullVersion Full form of previous version
     * @throws TException process exception
     */
    public abstract void replaceCurrentWithPrevious(int currentVersionID, File previousFullVersion)
        throws TException;
    /**
     * Build a full version directory for a specific version
     * @param versionID identifier of version to be reconstructed
     * @param fullVersion user supplied file to contain reconstructed version
     * @return true-file reconstructed; false-file not reconstructed
     * @throws org.cdlib.mrt.utility.MException
     */
    public abstract boolean getFullVersion(int versionID, File fullVersion)
        throws TException;


    /**
     * Build a temp full version directory for a specific version
     * @param versionID identifier of version to be reconstructed
     * @return temporary file containing version
     * @throws org.cdlib.mrt.utility.MException
     */
    public abstract File getFullVersion(int versionID)
        throws TException;
    
    /**
     * Reconstruct a full version from deltas
     * @param buildVersion
     * @param nextVersion - next version to be used for reconstruction
     * @throws org.cdlib.mrt.utility.MException
     */
    public abstract void buildFullVersion(
            File buildVersion,
            File nextVersion)
        throws TException;



    /**
     * Create full forms of all versions in a directory
     * @param objectFile directory to contain all version files
     * @return true=able to build directory, false=fails
     * @throws TException process exception
     */
    public abstract boolean saveFullVersions(File objectFile)
        throws TException;


    /**
     * get a File for the version
     * @param versionID used to get existing version directory
     * @return File corresponding to version directory
     * @throws org.cdlib.mrt.utility.MException
     */
    public File getVersionFile(int versionID)
        throws TException
    {
        try {
            String versionName = getVersionName(versionID);
            File versionFile = new File(m_objectStoreBase, versionName);
            if (!versionFile.exists()) return null;
            return versionFile;

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "getNextVersion - unable to find version. Exception:" + ex);
        }
    }

    /**
     * Get the current.txt version number
     * @return next version
     * @throws org.cdlib.mrt.utility.MException
     */
    public int getCurrentTxtVersion()
        throws TException
    {
        try {
            File currentTxt = new File(m_objectStoreBase, "current.txt");
            if (!currentTxt.exists()) return 0;
            String valueS = FileUtil.file2String(currentTxt);
            if (StringUtil.isEmpty(valueS) || (valueS.length() < 4)) return 0;
            valueS = valueS.substring(1);
            int value = Integer.parseInt(valueS);
            return value;

        } catch (Exception ex) {
            return 0;
        }
    }


    /***
     * create a new file based on the passed versionID
     * @param nextVersion version ID of file to be created
     * @return new File
     * @throws TException
     */
    protected File createVersionDirectory(
            int nextVersion)
        throws TException
    {
        try {
            String versionName = getVersionName(nextVersion);
            File version = new File(m_objectStoreBase, versionName);
            return version;

        } catch (Exception ex) {
            String msg = MESSAGE
                    + "createVersionDirectory - failed to create version directory."
                    + " Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }

    /**
     * get standard v(nnn) name of version based on versionID
     * @param versionID int form of versionID
     * @return v name
     */
    public static String getVersionName(int versionID)
    {
        return String.format("v%03d", versionID);
    }

    /**
     * Get the next version number
     * @return next version
     * @throws org.cdlib.mrt.utility.MException
     */
    public int getNextVersion()
        throws TException
    {
        try {
            File [] versions = m_objectStoreBase.listFiles();
            if ((versions == null) || (versions.length == 0)) return 1;
            int maxVersion = 0;
            for (int i=0; i < versions.length; i++) {
                File file = versions[i];
                if (!file.isDirectory()) continue;
                String fileName = file.getName();

                if (fileName.matches("[vV]\\d+")) {
                    int version = Integer.parseInt(fileName.substring(1));
                    if (version > maxVersion) maxVersion = version;
                }

            }
            maxVersion++;
            return maxVersion;

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "getNextVersion - unable to find version. Exception:" + ex);
        }
    }

    /**
     * Return this file based on versionID and fileID
     * @param versionID versionID of file
     * @param fileName component name of file
     * @return matched file
     * @throws TException
     */
    public abstract File getVersionFile(int versionID, String fileID)
        throws TException;

    protected void log(String msg, int lvl)
    {
        if (logger.getMessageMaxLevel() < lvl) return;
        //System.out.println(msg);
        logger.logMessage(msg, 0, true);
    }

    /**
     * Match current and previous file components to see if they
     * are the same. If the checksum type has changed then get the new
     * checksum based on the current checksum
     * @param previousState previous file description
     * @param currentState current file description
     * @return true=match, false=do not match
     * @throws TException
     */
    protected boolean matchFileStates(
            FileComponent previousState,
            FileComponent currentState)
        throws TException
    {
        boolean match = false;
        MessageDigest previousDigest = previousState.getMessageDigest();
        MessageDigest currentDigest = null;

        if (currentState != null) {
            currentDigest  = currentState.getMessageDigest();
            if (previousState.getSize() != currentState.getSize()) return false;
            if (currentDigest.getAlgorithm() != previousDigest.getAlgorithm()) {
                MessageDigestType currentType = currentDigest.getAlgorithm();
                MessageDigestValue digestValue = new MessageDigestValue(previousState.getComponentFile(),
                        currentType.toString(),
                        logger);
                String checksum = digestValue.getChecksum();

                if (currentDigest.getValue().equals(checksum)) {
                    return true;
                } else {
                    return false;
                }


            }
            if (previousDigest.toString().equals(currentDigest.toString())) {
                match = true;
            } else match = false;
        } else {
            match = false;
        }
        return match;
    }

    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    protected VersionContent getVersionContent(File versionFile)
            throws TException
    {
        try {
            File manifestFile = new File(versionFile, MANIFEST_TXT);
            if ((manifestFile == null) || !manifestFile.exists()) {
                String msg = MESSAGE
                    + "getVersionContent - manifest file missing.";
                throw new TException.INVALID_ARCHITECTURE( msg);
            }

            InputStream inputStream = new FileInputStream(manifestFile);
            
            Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.object);
            VersionContent versionContent = DflatUtil.getVersionContent(logger, manifest, inputStream);
            return versionContent;

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw DflatUtil.makeGeneralTException(logger, "getVersionContent", ex);
        }
    }

    /**
     * Build a full for the previous version
     * @param newPrevious
     * @param current
     * @param previous
     */
    protected void initBuildPrevious(
            File tempPrevious,
            File current,
            File previous)
        throws TException
    {
        if ((tempPrevious == null) || !tempPrevious.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "buildFullPrevious - tempPrevious file not defined");
        }
        if ((current == null) || !current.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "buildFullPrevious - current file not defined");
        }
        if ((previous == null) || !previous.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "buildFullPrevious - previous file not defined");
        }
        File currentFull = new File(current, FULL);
        if (!currentFull.exists()) {
            throw new TException.INVALID_ARCHITECTURE (
                    MESSAGE + "buildFullPrevious - currentFull must contain a full directory");
        }
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

}
