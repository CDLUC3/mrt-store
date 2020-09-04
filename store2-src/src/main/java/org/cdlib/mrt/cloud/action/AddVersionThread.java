/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
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
*******************************************************************************/
package org.cdlib.mrt.cloud.action;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.Callable;

import org.cdlib.mrt.cloud.utility.StoreMapStates;

import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;

import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class AddVersionThread
        extends AddVersionContentAbs
        implements Callable, Runnable
{

    protected static final String NAME = "AddVersionThread";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final boolean DEBUGLOW = false;
    protected static final String WRITE = "write";
    protected int threadCnt = 4;

    protected VersionState versionState = null;
    protected InputStream manifestInputStream = null;
    protected volatile TException threadTException = null;
    
    
    public static AddVersionThread getAddVersionThread(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateWrite,
            InputStream manifestInputStream,
            int threadCnt,
            LoggerInf logger)
        throws TException
    {
        return new AddVersionThread(s3service, bucket, objectID, validateWrite, manifestInputStream, threadCnt, logger);
    }
    
    protected AddVersionThread(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateWrite,
            InputStream manifestInputStream,
            int threadCnt,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, validateWrite, threadCnt, logger);
        setDEBUGOUT(false);
        if (manifestInputStream == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "AddVersion - no manifestInputStream");
        }
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
        if (DEBUG) System.out.println(map.dump("AddVersion"));
        this.manifestInputStream = manifestInputStream;
        if (DEBUG) {
            System.out.println(MESSAGE + "****start - " + DateUtil.getCurrentIsoDate());
        }
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            versionState = addVersion(objectID, manifestInputStream);

        } catch (Exception ex) {
            String msg = MESSAGE + "AddVersion Exception for "
                    + " - bucket=" + bucket
                    + " - objectID=" + objectID
                    + " - Exception:" + ex
                    ;
            logger.logError(msg, 2);
            setException(ex);

        }

    }

    public VersionState process()
        throws TException
    {
        run();
        if (exception != null) {
            log("Exception:" + exception);
            log(StringUtil.stackTrace(exception));
            if (exception instanceof TException) {
                throw (TException) exception;
            }
            else {
                throw new TException(exception);
            }
        }
        return versionState;
    }

    @Override
    public VersionState call()
    {
        run();
        return versionState;
    }


    public VersionState callEx()
        throws TException
    {
        return process();
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
        try {
            log(MESSAGE + "addVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            if (manifestInputStream == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "Manifest file does not exist");
            }
            
            contentAccumTime = 0;
            writeAccumTime = 0;
            validateAccumTime = 0;
            long startTime = DateUtil.getEpochUTCDate();
            Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
            ComponentContent inVersionContent = new ComponentContent(
                    logger,
                    manifest,
                    manifestInputStream);
            nextVersion = map.getNextVersion();
            writeContent(nextVersion, inVersionContent);
            if (exception != null) return null;
            writeVersionMap();
            int current = map.getCurrent();
            VersionState versionState = StoreMapStates.getVersionState(map, current);

            long endTime = DateUtil.getEpochUTCDate();
            String isoDate = DateUtil.getCurrentIsoDate();
            logger.logMessage("***addVersion timing cloud[" + objectID.getValue() + "," + isoDate + "]:"
                    + " - get=" + contentAccumTime
                    + " - out=" + writeAccumTime
                    + " - validate=" + validateAccumTime
                    + " - trans=" + (endTime - startTime),
                    15
            );
            return versionState;
            
        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    } 
    
    public void setFile(FileComponent manifestComponent)
        throws TException
    {
        try {
            if (manifestComponent.getComponentFile() != null) return;
            URL url = manifestComponent.getURL();
            if (url == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fillComponent - component URL missing");
            }
            File tmpFile = FileUtil.getTempFile("tmp", ".txt");
            FileUtil.url2File(logger, url, tmpFile);
            manifestComponent.setComponentFile(tmpFile);


        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }
}

