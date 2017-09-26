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
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.utility.StoreMapStates;


import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.utility.NormVersionMap;
import org.cdlib.mrt.store.ObjectState;

import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.NodeInf;
import static org.cdlib.mrt.store.action.ActionAbs.isTempManifestFile;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class CopyObject
        extends AddVersionContentAbs
        implements Callable, Runnable
{

    protected static final String NAME = "CopyObject";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected static final boolean DISPLAYMAP = true;
    protected static final String WRITE = "write";

    protected ObjectState objectState = null;
    protected boolean validateWrite = false;
    protected NodeInf fromNode = null;
    protected VersionMap inMap = null;
    
    public static CopyObject getCopyObject(
            CloudStoreInf s3service,
            String bucket,
            NodeInf fromNode,
            Identifier objectID,
            boolean validateWrite,
            int threadCnt,
            LoggerInf logger)
        throws TException
    {
        return new CopyObject(s3service, bucket, fromNode, objectID, validateWrite, threadCnt, logger);
    }
    
    protected CopyObject(
            CloudStoreInf s3service,
            String bucket,
            NodeInf fromNode,
            Identifier objectID,
            boolean validateWrite,
            int threadCnt,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, validateWrite, threadCnt, logger);
        this.validateWrite = validateWrite;
        this.fromNode = fromNode;
        if (DEBUG) {
            System.out.println(MESSAGE + "****start - " + DateUtil.getCurrentIsoDate());
        }
        
        inMap = getVersionMap(fromNode);
        if (DEBUG) System.out.println("CopyObject numberVersion=" + inMap.getCurrent());
    }
    
    public VersionMap getVersionMap(NodeInf fromNode)
        throws TException
    {
        File mapFile = null;
        try {
            FileContent mapContent = fromNode.getCloudManifest(objectID, false);
            mapFile = mapContent.getFile();
            InputStream manifestXMLIn = new FileInputStream(mapFile);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
            
        } finally {
            try {
                if (mapFile != null) {
                    if (isTempManifestFile(mapFile)) {
                        mapFile.delete();
                    }
                }
            } catch (Exception ex) {
                System.out.println("Delete mapFile exception:" + mapFile);
            }
        }
    }
    
    @Override
    public void run()
    {
        try {
            log("run entered");
            objectState = copyVersions();
            
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

    public ObjectState process()
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
        return objectState;
    }

    @Override
    public ObjectState call()
    {
        run();
        return objectState;
    }


    public ObjectState callEx()
        throws TException
    {
        return process();
    }
    
    public ObjectState copyVersions()
        throws TException
    {   
        try {
            log(MESSAGE + "copyVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            String inMapS = NormVersionMap.versionMap2String(inMap);
            if (DISPLAYMAP) System.out.println("*****InMap:\n" + inMapS);
            int current = inMap.getCurrent();
            for (int v=1; v<=current; v++) {
                if (DEBUG) System.out.println("copyversion process version:" + v);
                ComponentContent inVersionContent = inMap.getVersionContent(v);
                writeContent(v, inVersionContent);
                if (exception != null) return null;
            }
            writeVersionMap();
            String outMapS = NormVersionMap.versionMap2String(map);
            if (DISPLAYMAP) System.out.println("*****OutMap:\n" + outMapS);
            return StoreMapStates.getObjectState(map);

        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    } 
    
    public void setFile(FileComponent component) 
        throws TException
    {
        FileContent content = fromNode.getFile(objectID, nextVersion, component.getIdentifier());
        File componentFile = content.getFile();
        component.setComponentFile(componentFile);
    }
}

