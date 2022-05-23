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
import java.io.InputStream;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.utility.StoreMapStates;



import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.store.ObjectState;
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
public class DeleteObject
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "DeleteObject";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected StateInf returnState = null;
    protected int current = -1;
    
            
    public static DeleteObject getDeleteObject(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            LoggerInf logger)
        throws TException
    {
        return new DeleteObject(s3service, bucket, objectID, logger);
    }
    
    protected DeleteObject(
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
        System.out.println(map.dump("DeleteObject"));
    }


    @Override
    public void run()
    {
        try {
            log("run entered");
            returnState = call();

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
    public StateInf call()
    {
        StateInf localState = process();
        if (exception != null) {
            if (exception instanceof TException) {
                return (TException) exception;
            }
            else {
                return new TException(exception);
            }
        }
        return localState;
    }

    public StateInf callEx()
        throws TException
    {
        ObjectState objectState = process();
        
        if (exception != null) {
            if (exception instanceof TException) {
                throw (TException) exception;
            }
            else {
                throw new TException(exception);
            }
        }
        return objectState;
    }
    
    public ObjectState process ()
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
            ObjectState objectState = StoreMapStates.getObjectState(map);
            for (int i=current; i>=1; i--) {
                deleteCurrent(map);
                if (exception != null) return null;
                if (DEBUG) System.out.println("Delete current:" + i);
            }
            s3service.deleteManifest(bucket, objectID);
            return objectState;

        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    }
}

