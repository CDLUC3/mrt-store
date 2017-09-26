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
package org.cdlib.mrt.store;

import org.cdlib.mrt.cloud.utility.CloudUtil;
import org.cdlib.mrt.store.dflat.Dflat_1d0;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Factory: Object Store
 * @author dloy
 */
public abstract class ObjectStoreAbs
{

    protected LoggerInf logger = null;


    /**
     * Constructor
     * @param logger process logger
     */
    protected ObjectStoreAbs (
            LoggerInf logger)
    {
        this.logger = logger;
    }

    /**
     * Factory for Dflat
     * @param logger process logger
     * @return Dflat handler
     * @throws TException
     */
    public static Dflat_1d0 getDflat_1d0(LoggerInf logger)
            throws TException
    {
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                "Logger not resoved");
        }
        try{
            return new Dflat_1d0(logger);

        } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to instantiate ObjecStore: Exception" + ex, ex);
        }
    }

    /**
     * Get ObjectStore handler
     * @param logger process logger
     * @param dflatSpec version of Dflat
     * @return current version of dflat handler
     * @throws TException
     */
    public static ObjectStoreInf getObjectStore(LoggerInf logger, NodeState nodeState)
            throws TException
    {
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                "Logger not resoved");
        }
        String accessProtocol = nodeState.getAccessProtocol();
        SpecScheme dflatSpec = nodeState.retrieveLeafScheme();
        ObjectStoreInf objectStore = null;
        try{
            if ((accessProtocol != null) && accessProtocol.contains("s3")) {
                return setCloudStore(logger, nodeState);
            }
            switch(dflatSpec.getScheme()) {
                case dflat_1d0:
                    objectStore = getDflat_1d0(logger);
                    objectStore.setVerifyOnRead(nodeState.isVerifyOnRead());
                    objectStore.setVerifyOnWrite(nodeState.isVerifyOnWrite());
                    return objectStore;
            }

            return null;

        } catch (Exception ex) {
            ex.printStackTrace();
                throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to instantiate ObjecStore: Exception" + ex, ex);
        }
    }
    
    public static ObjectStoreInf setCloudStore(LoggerInf logger, NodeState nodeState)
            throws TException
    {
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                "Logger not resoved");
        }
        try{
            String bucket = nodeState.getLogicalVolume();
            String externalProvider = nodeState.getExternalProvider();
            System.out.println("***setCloudeStore"
                    + " - bucket:" + bucket
                    + " - externalProvider:" + externalProvider
            );
            if (externalProvider == null) {
                if (bucket.contains("/")) {
                    return CloudUtil.getFileCloudService(logger, bucket);
                }
                throw new TException.INVALID_OR_MISSING_PARM(
                    "externalProvider required for cloud service");
                
            } else if (nodeState.getExternalProvider().contains("sdsc")) {
                return CloudUtil.getOpenstackService(logger, bucket);
                
            } else if (nodeState.getExternalProvider().contains("aws")) {
                return CloudUtil.getAWSService(logger, bucket);
                
            } else if (nodeState.getExternalProvider().contains("nodeio")) {
                return CloudUtil.getNodeIOService(logger, bucket);
                
            } else {
                return null;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
                throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to instantiate CloudStore: Exception" + ex, ex);
        }
    }

    /**
     * write to process log
     * @param msg log entry
     * @param lvl vervosity lever
     */
    protected void log(String msg, int lvl)
    {
        //System.out.println(msg);
        logger.logMessage(msg, 0, true);
    }

}

