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
import java.util.concurrent.Callable;



import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.store.dflat.Dflat_1d0;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TallyTable;
/**
 * Run fixity
 * @author dloy
 */
public class ContentVersionArchive
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "ContentVersionArchive";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final boolean STATS = true;

    protected FileContent fileContent = null;
    protected DflatForm dflatForm = null;
    protected File workBase = null;
    protected Dflat_1d0 dflat = null;
    protected int versionID = -1;
    
    protected boolean validateRead = true;
    protected ComponentContent versionContent = null;
    protected boolean returnIfError = false;
    protected String archiveTypeS = null;
    protected TallyTable stats = new TallyTable();
    
    
    public static ContentVersionArchive getContentVersionArchive(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            LoggerInf logger)
        throws TException
    {
        return new ContentVersionArchive(s3service, bucket, objectID, versionID, returnIfError, archiveTypeS, logger);
    }    
    
    protected ContentVersionArchive(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            LoggerInf logger)
        throws TException
    {
        
        super(s3service, bucket, objectID, logger);
        this.returnIfError = returnIfError;
        this.archiveTypeS = archiveTypeS;
        this.versionID = versionID;
        workBase = FileUtil.getTempDir("tmp");
        long formTimeStart = getTime();
        dflatForm = new DflatForm(s3service, bucket, objectID, workBase, logger);
        bumpTime("builddflat", formTimeStart);
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
        int current = map.getCurrent();
        if (versionID == 0) {
            versionID = current;
        }
        if (versionID < 1) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "versionID not set null");
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
        long processTimeStart = getTime();
        try {
            log(MESSAGE + " entered"
                    + " - objectID=" + objectID
                    , 10);
            dflatForm.build();
            File objectStoreBase = dflatForm.getObjectStoreBase();
            FileContent fileContent = dflat.getVersionArchive(objectStoreBase, objectID, versionID, returnIfError, archiveTypeS);
            bumpTime("buildarchive", processTimeStart);
            return fileContent;
            
        } catch(Exception ex) {
            setException(ex);
            return null;
            
        } finally {
            deleteWorkBase(workBase);
            if (STATS) System.out.println("[TIMER-" + NAME + "]:" + stats.dump());
        }
    }  
    
    protected void bumpTime(String key, long startTime)
    {
        if (!STATS) return;
        stats.bump(key, startTime);
    }
    
    protected long getTime()
    {
        return System.nanoTime();
    }
    
}

