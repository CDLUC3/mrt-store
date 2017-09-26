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
import java.util.List;
import java.util.concurrent.Callable;

import org.cdlib.mrt.cloud.VersionMap;


import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.store.dflat.Dflat_1d0;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class ContentObjectLink
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "ContentObjectLink";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected StateInf returnState = null;
    protected FileContent fileContent = null;
    protected String baseManifestURL = null;
    protected DflatForm dflatForm = null;
    protected File workBase = null;
    protected Dflat_1d0 dflat = null;
            
    public static ContentObjectLink getContentObjectLink(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            String baseManifestURL,
            LoggerInf logger)
        throws TException
    {
        return new ContentObjectLink(s3service, bucket, objectID, baseManifestURL, logger);
    }    
    
    protected ContentObjectLink(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            String baseManifestURL,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        this.baseManifestURL = baseManifestURL;
        workBase = FileUtil.getTempDir("tmp");
        dflatForm = new DflatForm(s3service, bucket, objectID, workBase, logger);
        dflat = dflatForm.getDflat();
        map = dflatForm.getVersionMap();
        validate();
    }
    private void validate()
        throws TException
    {
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
        if (StringUtil.isEmpty(baseManifestURL)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            fileContent = call();

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
        fileContent = process();
        if (exception != null) {
            if (exception instanceof TException) {
                return null;
            }
            else {
                return null;
            }
        }
        return fileContent;
    }

    public FileContent callEx()
        throws TException
    {
        fileContent = process();
        throwEx();
        return fileContent;
    }
    
    public FileContent process ()
    {   
        try {
            log(MESSAGE + " entered"
                    + " - objectID=" + objectID
                    , 10);
            dflatForm.build();
            File objectStoreBase = dflatForm.getObjectStoreBase();
            FileContent localFileContent = dflat.getObjectLink(objectStoreBase, objectID, baseManifestURL);
            return localFileContent;
            
        } catch(Exception ex) {
            setException(ex);
            return null;
            
        } finally {
            deleteWorkBase(workBase);
        }
    }  
}

