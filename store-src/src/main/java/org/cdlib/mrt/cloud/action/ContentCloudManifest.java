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
import org.cdlib.mrt.core.FileContent;


import org.cdlib.mrt.store.ObjectState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StateInf;
/**
 * Run fixity
 * @author dloy
 */
public class ContentCloudManifest
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "ContentCloudManifest";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String WRITE = "write";

    protected File manifestFile = null;
    protected FileContent manifestFileContent = null;
    protected Identifier objectID = null;
    protected boolean validateContent = false;
    protected InputStream manifestInputStream = null;
    protected boolean validateWrite = false;
    
    public static ContentCloudManifest getContentCloudManifest(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validate,
            LoggerInf logger)
        throws TException
    {
        return new ContentCloudManifest(s3service, bucket, objectID, validate, logger);
    }
    
    protected ContentCloudManifest(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateContent,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        manifestFile = FileUtil.getTempFile("tmpman.", ".xml");
        this.objectID = objectID;
        this.validateContent = validateContent;
        validate();
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
            manifestFileContent = call();

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
    public FileContent call()
    {
        FileContent returnFileContent = process();
        if (exception != null) {
            return null;
        }
        return returnFileContent;
    }

    public FileContent callEx()
        throws TException
    {
        FileContent returnFileContent = process();
        throwEx();
        return returnFileContent;
    }
    
    public FileContent process ()
    {   
        try {
            if (validateContent) {
                map = getVersionMap(bucket, objectID);
                int filesValidate = doValidation();
                String msg = MESSAGE + "validation succeeds for objectID=" + objectID.getValue() + " - cnt=" + filesValidate;
                if (DEBUG) System.out.println(msg);
                log(msg, 5);
                
            }
            getCloudManifest(bucket, objectID, manifestFile);
            return FileContent.getFileContent(manifestFile, logger);

        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    }  
}

