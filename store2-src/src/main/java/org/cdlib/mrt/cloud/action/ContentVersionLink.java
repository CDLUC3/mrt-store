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
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.cloud.utility.CloudUtil;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowAdd;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.PreSignedState;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.FileUtil;
//import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class ContentVersionLink
        extends CloudActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "ContentVersionLink";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String WRITE = "write";

    protected FileContent fileContent = null;
    protected String baseManifestURL = null;
    protected File workBase = null;
    protected Boolean presign = false;
    //protected Dflat_1d0 dflat = null;
    protected int versionID = -1;
            
    public static ContentVersionLink getContentVersionLink(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String baseManifestURL,
            Boolean presign,
            LoggerInf logger)
        throws TException
    {
        return new ContentVersionLink(s3service, bucket, objectID, versionID, baseManifestURL, presign, logger);
    }    
    
    protected ContentVersionLink(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String baseManifestURL,
            Boolean presign,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        this.versionID = versionID;
        this.baseManifestURL = baseManifestURL;
        this.presign = presign;
        workBase = FileUtil.getTempDir("tmp");
        map = getVersionMap(bucket, objectID);
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
        process();
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
        process();
        throwEx();
        return fileContent;
    }
    
    
    public void process ()
    { 
        ManifestRowAbs.ManifestType manifestType = ManifestRowAbs.ManifestType.add;
        ArrayList<String> list = new ArrayList<>();
        try {  
            File outManifest = FileUtil.getTempFile("link", ".txt");
            log(MESSAGE + " entered"
                    + " - objectID=" + objectID
                    , 10);
            if (StringUtil.isEmpty(baseManifestURL)) {
                String msg = MESSAGE
                    + "getPOSTListManifest - base URL not provided";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }
            List<FileComponent> fileComponents = map.getVersionComponents(versionID);
            buildAddManifest(outManifest, fileComponents);
            fileContent = FileContent.getFileContent(outManifest, logger);

            
        } catch(Exception ex) {
            setException(ex);
        }
    }  
    
    
    
    /**
     * Build an add manifest from object manifest
     * @param versionID manifest version
     * @param outManifest manifest output file
     * @throws TException 
     */
    public void buildAddManifest(File outManifest, List<FileComponent> components)
        throws TException
    {
        try {
            if (DEBUG) {
                String mapXML = dumpVersionMap(map);
                System.out.println("\nXMLMAP=\n" + mapXML + "\n*****\n");
                
                System.out.println("\nMAP=\n" + map.dump("MAP HEADER"));
            }
            if (outManifest == null) {
                throw new TException.INVALID_OR_MISSING_PARM("outManifest required");
            }
            Manifest manOut = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
            manOut.openOutput(outManifest);

            ManifestRowAdd manRowOut
                = (ManifestRowAdd)ManifestRowAbs.getManifestRow(ManifestRowAbs.ManifestType.add, logger);
            if (DEBUG) System.out.println("components size=" + components.size());
            for (FileComponent component : components) {
                
                URL fileLink = getFileLink(component);
                component.setURL(fileLink);
                manRowOut.setFileComponent(component);
                manOut.write(manRowOut);
            }
            manOut.writeEOF();
            manOut.closeOutput();
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        }
    }
    
    protected URL getFileLink(FileComponent component)
        throws TException
    {
        URL fileLink = null;
        try {
            URL baseURL = new URL(baseManifestURL);
            fileLink = StoreUtil.buildContentURL(
                    null,
                    baseURL,
                    null,
                    objectID,
                    versionID,
                    getObjectID().getValue());
            if ((presign != null) && (presign == true)) {
                String contentType= component.getMimeType();
                String key = component.getLocalID();
                CloudResponse response = s3service.getPreSigned(120, bucket, key, contentType, null);
                if (response.getException() == null) {
                    fileLink = response.getReturnURL();
                }
            }
            return fileLink;
            
        } catch (Exception ex) {
            throw new TException.INVALID_DATA_FORMAT(MESSAGE
                    + "getPOSTListManifest"
                    + " - passed URL format invalid: baseURL=" + baseManifestURL
                    + " - Exception:" + ex);
        }
    }
}
