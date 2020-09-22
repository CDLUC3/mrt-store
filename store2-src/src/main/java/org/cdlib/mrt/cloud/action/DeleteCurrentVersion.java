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

import java.util.concurrent.Callable;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.utility.StoreMapStates;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class DeleteCurrentVersion
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "DeleteCurrentVersion";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String WRITE = "write";

    protected StateInf returnState = null;
    protected int current = -1;
    
            
    public static DeleteCurrentVersion getDeleteCurrentVersion(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            LoggerInf logger)
        throws TException
    {
        return new DeleteCurrentVersion(s3service, bucket, objectID, logger);
    }
    
    protected DeleteCurrentVersion(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
        map =  getVersionMap(bucket, objectID);
        current = map.getCurrent();
        if (DEBUG) System.out.println(map.dump("DeleteCurrentVersion"));
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            returnState = deleteCurrentVersion(objectID);

        } catch (Exception ex) {
            String msg = MESSAGE + "DeleteCurrent Exception for "
                    + " - bucket=" + bucket
                    + " - objectID=" + objectID
                    + " - Exception:" + ex
                    ;
            logger.logError(msg, 2);
            setException(ex);

        }

    }

    public StateInf process()
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
        return returnState;
    }

    @Override
    public StateInf call()
    {
        run();
        if (exception != null) {
            if (exception instanceof TException) {
                return (TException) exception;
            }
            else {
                return new TException(exception);
            }
        }
        return returnState;
    }

    public StateInf callEx()
        throws TException
    {
        return process();
    }

    public VersionState deleteCurrentVersion (
                Identifier objectID)
        throws TException
    {   
        try {
            log(MESSAGE + "deleteCurrentVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "objectID null");
            }
            
            int current = map.getCurrent();
            VersionState versionState = StoreMapStates.getVersionState(map, current);
            
            deleteCurrent(map);
            VersionMap.DeltaStats delta = map.getDeltaStats();
            versionState.setDeltaNumFiles(delta.deltaActualCount);
            versionState.setDeltaSize(delta.deltaActualSize);
            
            if (exception != null) return null;
            return versionState;

        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    }
    
    public VersionMap getVersionMap()
    {
        return map;
    }
}

