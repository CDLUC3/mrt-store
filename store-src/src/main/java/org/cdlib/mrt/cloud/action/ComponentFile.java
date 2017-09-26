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

import java.io.InputStream;
import java.util.concurrent.Callable;

import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.cloud.VersionMap;



import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;



import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.StateInf;
/**
 * Run fixity
 * @author dloy
 */
public class ComponentFile
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "StateFile";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected FileComponent component = null;
    protected Identifier objectID = null;
    protected int versionID = -1;
    protected String fileID = null;
    
    
    public static ComponentFile getComponentFile(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String fileID,
            LoggerInf logger)
        throws TException
    {
        return new ComponentFile(s3service, bucket, objectID, versionID, fileID, logger);
    }
    
    protected ComponentFile(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String fileID,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        if (versionID < 0) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "versionID not valid:" + versionID);
        }
        if (StringUtil.isEmpty(fileID)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fileID is empty");
        }
        map =  getVersionMap(bucket, objectID);
        if (versionID == 0) {
            versionID = map.getCurrent();
        }
        log(map.dump("StateFile"));
        this.objectID = objectID;
        this.versionID = versionID;
        this.fileID = fileID;
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
            component = call();

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
    public FileComponent call()
    {
        FileComponent component = process();
        return component;
    }
    
    

    public FileComponent callEx()
        throws TException
    {
        FileComponent component = call();
        throwEx();
        return component;
    }

    public FileComponent process ()
    {   
        try {
            log(MESSAGE + " entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    , 10);
            
            int current = map.getCurrent();
            if (current == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object does not exist:" + objectID.getValue());
            }
            if (versionID > current) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "requested version not found in"
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - current=" + current
                        );
            }
            return map.getFileComponent(versionID, fileID);

        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    }  
}

