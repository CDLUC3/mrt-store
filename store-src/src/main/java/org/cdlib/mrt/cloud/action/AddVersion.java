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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.utility.StoreMapStates;

import org.cdlib.mrt.cloud.ManInfo;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;

import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.cloud.utility.NormVersionMap;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class AddVersion
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "AddVersion";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String WRITE = "write";

    protected VersionState versionState = null;
    protected Identifier objectID = null;
    protected InputStream manifestInputStream = null;
    protected boolean validateWrite = false;
    protected NormVersionMap normVersionMap = null;
    
    
    public static AddVersion getAddVersion(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateWrite,
            NormVersionMap normVersionMap,
            InputStream manifestInputStream,
            LoggerInf logger)
        throws TException
    {
        return new AddVersion(s3service, bucket, objectID, validateWrite, normVersionMap, manifestInputStream, logger);
    }
    
    protected AddVersion(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateWrite,
            NormVersionMap normVersionMap,
            InputStream manifestInputStream,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        if (manifestInputStream == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "AddVersion - no manifestInputStream");
        }
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
        map =  getVersionMapAdd(bucket, objectID);
        if (DEBUG) System.out.println(map.dump("AddVersion"));
        this.normVersionMap = normVersionMap;
        this.manifestInputStream = manifestInputStream;
        this.objectID = objectID;
        this.validateWrite = validateWrite;
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            versionState = addVersion(objectID, manifestInputStream);

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
    public VersionState addVersion (
                Identifier objectID,
                InputStream manifestInputStream)
        throws TException
    {   
        try {
            log(MESSAGE + "addVersion entered"
                    + " - objectID=" + objectID
                    , 10);
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "objectID null");
            }
            if (manifestInputStream == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "Manifest file does not exist");
            }
            
            
            Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
            ComponentContent inVersionContent = new ComponentContent(
                    logger,
                    manifest,
                    manifestInputStream);
            writeContent(inVersionContent);
            if (exception != null) return null;
            int current = map.getCurrent();
            return StoreMapStates.getVersionState(map, current);

        } catch(Exception ex) {
            setException(ex);
            return null;
            
        }
    }
    
    public int writeContent (ComponentContent versionContent)
        throws TException
    {   
        try {
            List<FileComponent> inComponents = versionContent.getFileComponents();
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
                        if (validateWrite) {
                            try {
                                validateComponent(nextVersion, component);
                            } catch (Exception ex) {
                                putException = ex;
                            }
                        }
                    }
                    if (putException != null) {
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
            setCloudKey(map, objectID, nextVersion, inComponents);
            map.addVersion(inComponents);
            normVersionMap.normalize(map, bucket, objectID);
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
    
    
    protected void setCloudKey(
                VersionMap versionMap,
                Identifier objectID,
                int versionID,
                List<FileComponent> components)
        throws TException
    {
        try {
            for (FileComponent component : components) {
                String key = org.cdlib.mrt.s3.service.CloudUtil.getKey(objectID, versionID, component.getIdentifier(), false);
                component.setLocalID(key);
                versionMap.setCloudComponent(component, false);
            }
            
        } catch (Exception ex) {
            setException(ex);
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
    
    public static void main(String[] args) throws TException {
        
        ManInfo test = new ManInfo();
        InputStream propStream =  test.getClass().getClassLoader().
                getResourceAsStream("testresources/SDSC-Openstack.properties");
        if (propStream == null) {
            System.out.println("Unable to find resource");
            return;
        }
        Properties xmlProp = new Properties();
        try {
            xmlProp.load(propStream);
            LoggerInf logLocal = new TFileLogger(NAME, 50, 50);
            //CloudStoreInf s3service = SDSCCloud.getSDSC(xmlProp, logLocal);
            String bucketName = "dloy.bucket";
            CloudStoreInf s3service = OpenstackCloud.getOpenstackCloud(xmlProp, logLocal);
            //Identifier objectID = new Identifier("ark:/13030/fdhij");
            Identifier objectID = new Identifier("ark:/13030/zyxwv");
            File manifestFile = new File("C:/Documents and Settings/dloy/My Documents/tomcat/tomcat-7.0-28080/webapps/test/s3/qt1k67p66s/manifest3.txt");
            if (manifestFile.exists()) System.out.println("File exists");
            InputStream manifestInputStream = new FileInputStream(manifestFile);
            NormVersionMap normVersionMap = new NormVersionMap(s3service, logLocal);
            AddVersion add = new AddVersion(s3service, bucketName, objectID, true, normVersionMap, manifestInputStream, logLocal);
            VersionState state = add.process();
            System.out.println("\n**************************\nVersionState=" + add.formatXML(state));
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
        
    }
}

