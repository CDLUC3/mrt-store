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
import java.util.concurrent.Callable;

import org.cdlib.mrt.cloud.VersionMap;



import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.MessageDigest;


import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StateInf;
/**
 * Run fixity
 * @author dloy
 */
public class ContentFile
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "ContentFile";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String WRITE = "write";

    protected File returnFile = null;
    protected Identifier objectID = null;
    protected int versionID = -1;
    protected String fileID = null;
    protected boolean validateRead = false;
    
    
    public static ContentFile getContentFile(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String fileID,
            boolean validateRead,
            LoggerInf logger)
        throws TException
    {
        return new ContentFile(s3service, bucket, objectID, versionID, fileID, validateRead, logger);
    }
    
    protected ContentFile(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String fileID,
            boolean validateRead,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        if (versionID < 0) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "versionID not valid:" + versionID);
        }
        map =  getVersionMap(bucket, objectID);
        int current = map.getCurrent();
        if (current == 0) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object does not exist:" + objectID.getValue());
        }
        if (versionID == 0) {
            versionID = map.getCurrent();
        }
        log(map.dump("ContentFile"));
        this.objectID = objectID;
        this.versionID = versionID;
        this.fileID = fileID;
        this.validateRead = validateRead;
        validate();
    }
    private void validate()
        throws TException
    {
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
        int current = map.getCurrent();
        if (versionID > current) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "requested version not found in"
                    + " - objectID=" + objectID.getValue()
                    + " - versionID=" + versionID
                    + " - current=" + current
                    );
        }
        if (StringUtil.isEmpty(fileID)) {
            
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fileID missing"
                    + " - objectID=" + objectID.getValue()
                    + " - versionID=" + versionID
                    );
        }
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            returnFile = call();

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
    public File call()
    {
        File localFile = process();
        if (exception != null) {
            return null;
        }
        return localFile;
    }

    public File callEx()
        throws TException
    {
        File file = call();
        throwEx();
        return file;
    }
    
    public File process ()
    {   
        File tmpFile = null;
        try {
            ComponentContent content = map.getVersionContent(versionID);
            if (content == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object not found "
                        + " - bucket=" + bucket
                        + " - objectID=" + objectID.getValue()
                        );
            }
            FileComponent component = content.getFileComponent(fileID);
            if (component == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "component not found in this version "
                        + " - bucket=" + bucket
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + fileID
                        );
            }
            log(MESSAGE + " entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileID=" + fileID
                    + " - key=" + component.getLocalID()
                    , 10);
            tmpFile = FileUtil.getTempFile("tmpcld.", ".txt");
            String key = component.getLocalID();
            CloudResponse response = new CloudResponse(bucket, key);
            s3service.getObject(bucket, key, tmpFile, response);
            Exception ex = response.getException();
            if (ex != null) {
                System.out.println(MESSAGE + "Exception:" + ex);
                throw ex;
            }
            if (validateRead) {
                component.setComponentFile(tmpFile);
                validate(versionID, component);
            }
            return tmpFile;

        } catch(Exception ex) {
            if (tmpFile != null) {
                try {
                    tmpFile.delete();
                } catch (Exception tex) { }
            }
            setException(ex);
            return null;
            
        }
    }
    
    protected void validate(int versionID, FileComponent file)
        throws TException
    {
        try {
            File componentFile = file.getComponentFile();
            MessageDigest digest = file.getMessageDigest();
            String checksumType = digest.getJavaAlgorithm();
            String checksum = digest.getValue();
            FixityTests tests = new FixityTests(componentFile, checksumType, logger);
            
            FixityTests.FixityResult fixityResult 
                    = tests.validateSizeChecksum(checksum, checksumType, file.getSize());
            FixityTests.Result result= tests.getResult();
            String msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + file.getIdentifier()
                        + " - checksumType=" + checksumType
                        + " - manifestChecksum=" + checksum
                        + " - manifestSize=" + file.getSize()
                        + " - returnChecksum=" + result.checksum
                        + " - returnSize=" + result.inputSize
                        + " - " + fixityResult.dump("ValidateComponent");
            if (!fixityResult.fileSizeMatch || !fixityResult.checksumMatch) {
                throw new TException.FIXITY_CHECK_FAILS("Fixity test fails for " + msg);
            } else {
                if (DEBUG) System.out.println("validateComponent OK:" + msg);
            }
            
        } catch (TException tex) {
            throw tex;

        } catch(Exception ex) {
            throw new TException(ex);
            
        }
    }
}

