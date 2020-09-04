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

import org.cdlib.mrt.cloud.utility.StoreMapStates;


import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;

import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class UpdateVersion
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "UpdateVersion";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String WRITE = "write";

    protected VersionState versionState = null;
    protected Identifier objectID = null;
    protected InputStream manifestInputStream = null;
    protected boolean validateWrite = false;
    protected String [] deleteList = null;
    
    
    public static UpdateVersion getUpdateVersion(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateWrite,
            InputStream manifestInputStream,
            String [] deleteList,
            LoggerInf logger)
        throws TException
    {
        return new UpdateVersion(s3service, bucket, objectID, validateWrite, manifestInputStream, deleteList, logger);
    }
    
    protected UpdateVersion(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateWrite,
            InputStream manifestInputStream,
            String [] deleteList,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
        map =  getVersionMapAdd(bucket, objectID);
        System.out.println(map.dump("UpdateVersion"));
        this.manifestInputStream = manifestInputStream;
        this.objectID = objectID;
        this.validateWrite = validateWrite;
        this.deleteList = deleteList;
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            versionState = updateVersion(objectID, manifestInputStream, deleteList);

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

    public VersionState process()
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
        return versionState;
    }

    @Override
    public VersionState call()
    {
        run();
        return versionState;
    }


    public VersionState callEx()
        throws TException
    {
        return process();
    }

    /**
     * Add new version
     * @param objectID of version to be added
     * @param manifestInputStream stream for dflatpost manifest
     * @return VersionState for add function
     * @throws org.cdlib.mrt.utility.MException
     */
    public VersionState updateVersion (
                Identifier objectID,
                InputStream manifestInputStream,
                String [] deleteList)
        throws TException
    {   
        try {
            log(MESSAGE + "updateVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "objectID null");
            }
            
            ComponentContent inVersionContent = null;
            if (manifestInputStream != null) {
                Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
                inVersionContent = new ComponentContent(
                        logger,
                        manifest,
                        manifestInputStream);
            }
            process(inVersionContent, deleteList);
            if (exception != null) return null;
            int current = map.getCurrent();
            return StoreMapStates.getVersionState(map, current);

        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    }
    
    public int process(ComponentContent versionContent,
                String [] deleteList)
        throws TException
    {   
        try {
            List<FileComponent> inComponents = null;
            if (versionContent != null) {
                writeContent(versionContent);
                inComponents = versionContent.getFileComponents();
            }
            map.updateVersion(inComponents, deleteList);
            File newManifest = buildXMLManifest (map, null);
            s3service.deleteManifest(bucket, objectID);
            s3service.putManifest(bucket, objectID, newManifest);
            newManifest.delete();
            return map.getCurrent();

        } catch(Exception ex) {
            setException(ex);
            return 0;
        }
    }
    
    public int writeContent (ComponentContent versionContent)
        throws TException
    {   
        try {
            List<FileComponent> inComponents = versionContent.getFileComponents();
            if (DEBUG) System.out.println("writeContent entered - cnt=" + inComponents.size());
            int nextVersion = map.getNextVersion();
            for (FileComponent component: inComponents) {
                if (component.getCreated() == null) {
                    component.setCreated(new DateState());
                }
                map.setCloudComponent(component, true);
                if (DEBUG) System.out.println("writeContent:"
                        + " - bucket:" + bucket
                        + " - objectID:" + objectID.getValue()
                        + " - versionID:" + nextVersion
                        + " - fileID:" + component.getIdentifier()
                        + " - key:" + component.getLocalID()
                        );
                if (DEBUG) System.out.println(component.dump("writeContent"));
                if (component.getLocalID() == null) {
                    CloudResponse response = s3service.putObject(
                            bucket, 
                            objectID, 
                            nextVersion, 
                            component.getIdentifier(), 
                            component.getComponentFile());
                    Exception putException = response.getException();
                    //!!!
                    if (false) {
                        if (component.getIdentifier().equals("producer/mrt-erc.txt")) putException = new TException.GENERAL_EXCEPTION("test");
                    }
                    
                    if (putException == null) {
                        if (DEBUG) System.out.println(MESSAGE + "validateComponent:" + validateWrite);
                        if (validateWrite) {
                            try {
                                validateComponent(nextVersion, component);
                            } catch (Exception ex) {
                                putException = ex;
                            }
                        }
                    } else {
                        if (DEBUG) System.out.println(MESSAGE + "BACKOUT exception=" + putException.toString());
                        backOutPut(versionContent);
                        if (putException instanceof TException) {
                            throw (TException) putException;
                        } else {
                            throw new TException(putException);
                        }
                    }
                    String retKey = response.getStorageKey();
                    if (response.getException() != null) {
                        throw response.getException();
                    }
                    component.setLocalID(retKey);
                    component.setTitle(WRITE);
                    component.getComponentFile().delete();
                }
            }
            return map.getCurrent();

        } catch(Exception ex) {
            setException(ex);
            return 0;
        }
    }
    

    
    public void backOutPut(ComponentContent versionContent)
        throws TException
    {   
        try {
            List<FileComponent> inComponents = versionContent.getFileComponents();
            for (FileComponent component: inComponents) {
                if ((component.getTitle() != null) && component.getTitle().equals(WRITE)) {
                    String key = component.getLocalID();
                    CloudResponse delResponse = s3service.deleteObject(bucket, key);
                    String status = "OK";
                    if (delResponse.getException() != null) status="FAIL";
                    log("backOutException DELETE key=" + key + " - status=" + status);
                }
            }

        } catch(Exception ex) {
            setException(ex);
        }
    }    
}

