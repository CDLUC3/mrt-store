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
import java.util.List;
import java.util.concurrent.Callable;
import static org.cdlib.mrt.cloud.action.FixityFile.MESSAGE;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FileComponent;



import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudResponse;


import org.cdlib.mrt.store.dflat.Dflat_1d0;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.store.ObjectFixityState;
import org.cdlib.mrt.store.action.ArchiveComponent;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TallyTable;
/**
 * Run fixity
 * @author dloy
 */
public class FixityObject
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "FixityObject";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected ObjectFixityState objectFixityState = null;
    protected int testCnt = 0;
    protected int exCnt = 0;
    protected int errCnt = 0;
    
    public static FixityObject getFixityObject(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            LoggerInf logger)
        throws TException
    {
        return new FixityObject(s3service, bucket, objectID,  logger);
    }
    
    protected FixityObject(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        System.out.println("FixityObject"
                + " - bucket=" + bucket
                + " - objectID=" + objectID.getValue()
        );
        validate();
        map = getVersionMap(bucket, objectID);
        objectFixityState = new ObjectFixityState()
                .setObjectID(objectID)
                .setVersionCnt(map.getCurrent());
    }
    private void validate()
        throws TException
    {
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            ObjectFixityState retObjectFixityState = call();

        } catch (Exception ex) {
            String msg = MESSAGE + "Exception for "
                    + " - bucket=" + bucket
                    + " - objectID=" + objectID
                    + " - Exception:" + ex
                    ;
            logger.logError(msg, 2);
            setException(ex);

        }

    }

    @Override
    public ObjectFixityState call()
    {
        Integer retCnt = process();
        if (exception != null) {
            if (exception instanceof TException) {
                return null;
            }
            else {
                return null;
            }
        }
        return objectFixityState;
    }

    public ObjectFixityState callEx()
        throws TException
    {
        Integer retCnt = process();
        throwEx();
        return objectFixityState;
    }
    
    public Integer process()
    {   
        int current = map.getCurrent();
        if (current == 0) {
            return testCnt;
        }
        try {
            log(MESSAGE + " entered"
                    + " - objectID=" + objectID
                    , 10);
            for (int version=1; version <= current; version++) {
                List<FileComponent> components= map.getVersionComponents(version);
                for (FileComponent component  : components) {
                    String key = component.getLocalID();
                    if (!key.contains("|" + version + "|")) continue;
                    boolean addState = false;
                    FileFixityState fileFixityState = getFileFixity(version, component);
                    if (fileFixityState == null) {
                        addState = true;
                        exCnt++;
                    }
                    if (!fileFixityState.isSizeMatches() || !fileFixityState.isDigestMatches()) {
                        addState = true;
                        errCnt++;
                    }
                    if (addState) {
                        objectFixityState.add(fileFixityState);
                    }
                    testCnt++;
                }
                objectFixityState.setErrorCnt(errCnt);
                objectFixityState.setFileCnt(testCnt);
                objectFixityState.setExCnt(exCnt);
                objectFixityState.setVersionCnt(current);
                if ((errCnt == 0) && (exCnt == 0)) objectFixityState.setMatch(true);
            }
            
        
            return testCnt;
            
        } catch(Exception ex) {
            setException(ex);
            return null;
        }
    }   
    
    protected FileFixityState getFileFixity (int versionID, FileComponent component)
    {   
        File tmpFile = null;
        FileFixityState state = new FileFixityState();
        try {
            if (component == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "component not found in this version "
                        + " - bucket=" + bucket
                        + " - objectID=" + objectID.getValue()
                        );
            }
            String fileID = component.getIdentifier();
            state.setObjectID(objectID);
            state.setVersionID(versionID);
            state.setFileName(fileID);
            state.setFileDigest(component.getMessageDigest());
            state.setFileSize(component.getSize());
            state.setFixityDate(new DateState());
            state.setKey(component.getLocalID());
            String checksumTypeManifest = component.getMessageDigest().getJavaAlgorithm();
            String checksumManifest = component.getMessageDigest().getValue();
            log(MESSAGE + " entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileID=" + fileID
                    + " - key=" + component.getLocalID()
                    , 10);
            tmpFile = FileUtil.getTempFile("tmp", ".txt");
            String key = component.getLocalID();
            CloudResponse response = new CloudResponse(bucket, objectID, versionID, fileID);
            InputStream in = s3service.getObject(bucket, key, response);
            if (in == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Unable to access:"
                    + " - bucket:" + bucket
                    + " - key=" + component.getLocalID()
                );
            }
            FileUtil.stream2File(in, tmpFile);
            MessageDigestValue mdv = new MessageDigestValue(tmpFile, checksumTypeManifest, logger);
            String checksumFile = mdv.getChecksum();

            String md5 = response.getMd5();

            if (tmpFile.length() != response.getStorageSize()) {
                state.setSizeMatches(false);
            } else {
                state.setSizeMatches(true);
            }


            if (!checksumManifest.equals(checksumFile)) {
                state.setDigestMatches(false);
            } else {
                state.setDigestMatches(true);
                log("Validation match "
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + fileID
                        + " - checksumTypeManifest=" + checksumTypeManifest
                        + " - saveChecksum=" + checksumManifest
                        + " - foundChecksum=" + checksumFile
                        );
            }
            
            return state;

        } catch(Exception ex) {
            state.setEx(ex);
            ex.printStackTrace();
            return state;
            
        } finally {
            if (tmpFile != null) {
                try {
                    tmpFile.delete();
                } catch (Exception ex) { }
            }
        }
    } 
    
    protected long getTime()
    {
        return System.nanoTime();
    }
}

