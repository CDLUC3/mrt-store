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

import org.cdlib.mrt.store.ObjectStoreInf;
import org.cdlib.mrt.store.ObjectStoreAbs;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.store.NodeInf;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Dflat 1.0
 * @author dloy
 */
public class Dflat_1d0
        extends ObjectStoreAbs
{
    protected static final String NAME = "Dflat";
    protected static final String MESSAGE = NAME + ": ";

    protected static final String NL =  System.getProperty("line.separator");
    protected static String DFLATINFOTXT = ""
        + "objectScheme: Dflat/0.20/1.0" + NL
        + "manifestScheme: Checkm/0.1" + NL
        + "fullScheme: Dnatural/0.19" + NL
        + "deltaScheme: ReDD/0.1/1.0" + NL
        + "currentScheme: file" + NL
        + "classScheme: CLOP/0.3" + NL
        ;

    protected DflatInfo dflatInfo = null;
    protected static final SpecScheme dflatSpec = new SpecScheme(SpecScheme.Enum.dflat_1d0, null);

    protected boolean verifyOnRead = true;
    protected boolean verifyOnWrite = true;
    
    //private DflatManager m_dflatManager = null;

    /**
     * Create a new DflatInfo  based on data specified charactersistics
     * @return 1.0 version of local dflat
     * @throws TException
     */
    public static DflatInfo getDflatInfoTxt()
        throws TException
    {
        return new DflatInfo(DFLATINFOTXT);
    }

    /**
     * Contructor
     * @param logger process log
     * @throws TException processing exception
     */
    public Dflat_1d0 (LoggerInf logger)
        throws TException
    {
        super(logger);
        dflatInfo = new DflatInfo(DFLATINFOTXT);
        log(MESSAGE + "instantiation entered", 10);
    }

    //@Override
    public VersionState addVersion (
                File objectStoreBase,
                Identifier objectID,
                File manifestFile)
        throws TException
    {
        try {
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileInputStream inStream = new FileInputStream(manifestFile);
            VersionState versionState = dflatManager.addVersion(objectID, inStream);
            log(MESSAGE + "addVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            return versionState;
        
        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }
    
    //@Override
    public VersionState updateVersion (
            File objectStoreBase,
            Identifier objectID,
            File manifestFile,
            String [] deleteList)
    throws TException
    {
        try {
            FileInputStream inStream = null;
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            if ((manifestFile != null) && manifestFile.exists()) {
                System.out.println(MESSAGE + "updateVersion - manifestFile exists:" + manifestFile.getCanonicalPath());
                inStream = new FileInputStream(manifestFile);
            }
            VersionState versionState = dflatManager.updateVersion(objectID, inStream, deleteList);
            log(MESSAGE + "updateVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            return versionState;
        
        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
    public ObjectState copyObject (
            File storeFile,
            NodeInf fromNode,
            Identifier objectID)
    throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE("copyObject not currently supported when toNode is d-flat");
    }

    //@Override
    public VersionState deleteVersion (
                File objectStoreBase,
                Identifier objectID,
                int versionID)
        throws TException
    {
        try {
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            VersionState versionState = dflatManager.deleteVersion(objectID, versionID);
            log(MESSAGE + "deleteVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            return versionState;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
    public ObjectState deleteObject (
            File objectStoreBase,
            Identifier objectID)
    throws TException
    {
 
        ObjectState objectState = null;
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "deleteObject - objectID required");
            }
            objectState = getObjectState(objectStoreBase,objectID);

            FileUtil.deleteDir(objectStoreBase);
            objectState.setObjectID(objectID);
            return objectState;

        } catch (Exception ex) {
            throw makeTException(ex);

        }
    }


    public SpecScheme getSpecScheme()
    {
        return dflatSpec;
    }

    //@Override
    public VersionContent getVersionContent (
            File objectStoreBase,
            Identifier objectID,
            int versionID)
        throws TException
    {
        try {
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            VersionContent versionContent = dflatManager.getVersionContentFromManifestFile(versionID);
            versionContent.setObjectID(objectID);
            log(MESSAGE + "putVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            return versionContent;

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
    public ObjectState getObjectState (
            File objectStoreBase,
            Identifier objectID)
        throws TException
    {
        try {
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            ObjectState objectState = dflatManager.getObjectState(objectID);
            log(MESSAGE + "getObjectState entered"
                    + " - objectID=" + objectID
                    , 10);
            return objectState;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }


    //@Override
    public VersionState getVersionState (
            File objectStoreBase,
            Identifier objectID,
            int versionID)
        throws TException
    {
        try {
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            VersionState versionState = dflatManager.getVersionState(versionID);
            log(MESSAGE + "getVersionState entered"
                    + " - objectID=" + objectID
                    , 10);
            return versionState;

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
    public FileComponent getFileState (
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        try {
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileComponent fileState = dflatManager.getFileState(versionID, fileName);
            if (fileState == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        "Unable to locate file: " + fileName);
            }
            log(MESSAGE + "getFileState entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileName
                    + " - dump=" + fileState.dump("")
                    , 10);
            return fileState;

       } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
    public FileFixityState getFileFixityState (
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        try {
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileFixityState fileFixityState = dflatManager.getFileFixityState(versionID, fileName);
            if (fileFixityState == null) {
                throw new TException.INVALID_ARCHITECTURE(
                        "Unable to locate file:" + fileName);
            }
            log(MESSAGE + "FileFixityState entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileName
                    + " - dump=" + fileFixityState.dump("")
                    , 10);
            return fileFixityState;

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
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

            String [] parts = key.split("\\|");
            if (parts.length < 3) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - key invalid:" + key);
            }
            Identifier objectID = new Identifier(parts[0]);
            int versionID = -1;
            try {  
                versionID = Integer.parseInt(parts[1]);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - version invalid:" + key);
            }
            String fileName = parts[2];
            
            File returnFile = getFile(objectStoreBase, objectID, versionID, fileName);
            FileUtil.file2file(returnFile, outFile);

        }  catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
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
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            File file = dflatManager.getFileFixity(versionID, fileName);
            return file;

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }
    
    //@Override
    public void getFileStream (
            File objectStoreBase,
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
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            File file = dflatManager.getFileFixity(versionID, fileName, false);
            if (file == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "item not found: "
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileName
                        );
            }
            FileInputStream fileStream = new FileInputStream (file);
            FileUtil.stream2Stream(fileStream, outputStream);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }

    //@Override
    public FileContent getObject(
            File objectStoreBase,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        try {
            log(MESSAGE + "getFile entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileContent fileContent = dflatManager.getObject(
                    objectID,
                    returnFullVersion,
                    returnIfError,
                    archiveTypeS);
            FileComponent fileState = fileContent.getFileComponent();

            File outFile = fileContent.getFile();
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }

    //@Override
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
            log(MESSAGE + "getFile entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            dflatManager.getObjectStream(
                    objectID,
                    returnFullVersion,
                    returnIfError,
                    archiveTypeS,
                    outputStream);

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }


    //@Override
    public FileContent getVersionArchive(
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException
    {
        try {
            log(MESSAGE + "getFile entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileContent fileContent = dflatManager.getVersionArchive(versionID, returnIfError, archiveTypeS);
            FileComponent fileState = fileContent.getFileComponent();

            File outFile = fileContent.getFile();
            return fileContent;

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }


    //@Override
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
            log(MESSAGE + "getFile entered"
                    + " - objectID=" + objectID
                    + " - archiveTypeS=" + archiveTypeS
                    , 10);
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            dflatManager.getVersionArchiveStream(versionID, returnIfError, archiveTypeS, outputStream);

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }

    //@Override
    public FileContent getVersionLink(
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            String linkBaseURL)
        throws TException
     {
        try {
            log(MESSAGE + "getVersionLink entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - linkBaseURL=" + linkBaseURL
                    , 10);
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileContent manifestFile = dflatManager.getVersionLink(objectID, versionID, linkBaseURL);
            return manifestFile;

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }

    //@Override
    public FileContent getObjectLink(
            File objectStoreBase,
            Identifier objectID,
            String linkBaseURL)
        throws TException
     {
        try {
            log(MESSAGE + "getVersionLink entered"
                    + " - objectID=" + objectID
                    + " - linkBaseURL=" + linkBaseURL
                    , 10);
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileContent manifestFile = dflatManager.getObjectLink(objectID,linkBaseURL);
            return manifestFile;

        } catch (Exception ex) {
            throw makeTException(ex);
        }

    }
    
    //@Override
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
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            FileContent manifestFile = dflatManager.getCloudManifest(objectID, validate);
            return manifestFile;

        } catch (Exception ex) {
            throw makeTException(ex);
        }
    }
    
    //@Override
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
            DflatManager dflatManager = getDflatManager(objectStoreBase, logger, dflatInfo);
            dflatManager.getCloudManifestStream(objectID, validate, outStream);

        } catch (Exception ex) {
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

    protected DflatManager getDflatManager(
            File objectStoreBase,
            LoggerInf logger,
            DflatInfo dflatInfo)
        throws TException
    {
        DflatManager dflatManager = new DflatManager(
                objectStoreBase,
                logger,
                dflatInfo,
                verifyOnRead,
                verifyOnWrite
                );
        logger.logMessage(MESSAGE + "getDflatManager"
                + " - verifyOnRead=" + verifyOnRead
                + " - verifyOnWrite=" + verifyOnWrite
                , 10);
        return dflatManager;
    }

    protected TException makeTException(Exception ex)
    {
        TException tex = null;
        if (ex instanceof TException) {
            tex = (TException)ex;
        } else {
            tex = new TException.GENERAL_EXCEPTION(ex);
        }
        logger.logError(tex.toString(), 0);
        logger.logError(tex.dump(MESSAGE), 20);
        return tex;
    }
    
    //@Override
    public CloudStoreInf getCloudService()
    {
        return null;
    }
    
    //@Override
    public String getCloudBucket()
    {
        return null;
    }
}

