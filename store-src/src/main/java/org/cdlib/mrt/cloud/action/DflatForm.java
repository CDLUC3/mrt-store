/*
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
**********************************************************/
package org.cdlib.mrt.cloud.action;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.store.dflat.Dflat_1d0;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowAdd;
import org.cdlib.mrt.store.dflat.DflatManager;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
/**
 * This object imports the formatTypes.xml and builds a local table of supported format types.
 * Note, that the ObjectFormat is being deprecated and replaced by a single format id (fmtid).
 * This change is happening because formatName is strictly a description and has no functional
 * use. The scienceMetadata flag is being dropped because the ORE Resource Map is more flexible
 * and allows for a broader set of data type.
 * 
 * @author dloy
 */
public class DflatForm
        extends CloudActionAbs
{
    private static final String NAME = "DflatForm";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    
    protected Dflat_1d0 dflat = null;
    protected File workBase = null;
    protected File objectStoreBase = null;
    protected File copyBase = null;
    

    public DflatForm(
            CloudStoreInf s3service, 
            String bucket, 
            Identifier objectID, 
            File workBase, 
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        map =  getVersionMap(bucket, objectID);
        this.workBase = workBase;
        validate();
    }

    private void validate()
        throws TException
    {
        try {
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger is null");
            }
            if (workBase == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "workBase is null");
            }
            if (!workBase.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "workBase does not exist:" + workBase.getCanonicalPath());
            }
            copyBase = new File(workBase, "copy");
            if (copyBase.exists()) {
                FileUtil.deleteDir(copyBase);
            }
            copyBase.mkdir();
            
            objectStoreBase = new File(workBase, "store");
            if (objectStoreBase.exists()) {
                FileUtil.deleteDir(objectStoreBase);
            }
            objectStoreBase.mkdir();
            
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID is null");
            }
            if (StringUtil.isEmpty(bucket)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "bucket name is null");
            }
            dflat = new Dflat_1d0(logger);
            
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
        
    }

    public void build()
        throws TException
    {
        try {
            
            int current = map.getCurrent();
            if (current == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Object contains no versions");
            }
            for (int xver=1; xver <= current; xver++) {
                File versionDir = new File(copyBase, "" + xver);
                versionDir.mkdir();
                File manifestFile = new File(versionDir, "manifest.txt");
                addDflatVersion(xver, manifestFile);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        }
        
    }
    
    public void addDflatVersion(int versionID, File outManifest) 
        throws TException
    {
        try {
            buildAddManifest(versionID, outManifest);
            dflat.addVersion(objectStoreBase, objectID, outManifest);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        }
        
    }
    
    /**
     * Build an add manifest from object manifest
     * @param versionID manifest version
     * @param outManifest manifest output file
     * @throws TException 
     */
    public void buildAddManifest(int versionID, File outManifest)
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
            List<FileComponent> components = map.getVersionComponents(versionID);
            if (DEBUG) System.out.println("components size=" + components.size());
            for (FileComponent component : components) {
                String key = component.getLocalID();
                String fileID = versionID + "/" + component.getIdentifier();
                if (DEBUG) System.out.println("fileID=" + fileID);
                File saveFile =  DflatManager.getComponentFile(copyBase, fileID);
                if (DEBUG) System.out.println("savFile=" + saveFile.getCanonicalPath());
                URL fileURL = saveFile.toURI().toURL();
                component.setURL(fileURL);
                CloudResponse response = new CloudResponse(bucket, objectID, versionID, fileID);
                InputStream inStream = s3service.getObject(bucket, key, response);
                FileUtil.stream2File(inStream, saveFile);
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

    public Dflat_1d0 getDflat() {
        return dflat;
    }
    
    public VersionMap getVersionMap() 
    {
        return map;
    }

    public File getObjectStoreBase() {
        return objectStoreBase;
    }
    
    
}
