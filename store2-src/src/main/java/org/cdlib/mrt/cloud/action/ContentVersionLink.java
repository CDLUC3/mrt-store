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
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowAdd;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.PreSignedState;
import org.cdlib.mrt.store.StorageConfig;
import static org.cdlib.mrt.store.app.jersey.JerseyBase.setBoolean;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.FileUtil;
//import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.json.JSONObject;
import org.json.JSONArray;
/**
 * Run fixity
 * @author dloy
 */
public class ContentVersionLink
        extends CloudActionAbs
        implements Callable, Runnable
{

    public static enum LocalFilterType {nosystem, producer, none};
    public static enum LocalOutputFormatType {invman, storeman, json};
    
    public static final String NAME = "ContentVersionLink";
    public static final String MESSAGE = NAME + ": ";
    public static final boolean DEBUG = false;
    public static final String WRITE = "write";

    public FileContent fileContent = null;
    public String baseManifestURL = null;
    public File workBase = null;
    public Boolean presign = false;
    public Boolean update = false;
    //public Dflat_1d0 dflat = null;
    public int versionID = -1;
    
    public int currentVersion = 0;
    public int localVersion = 0;
    public int versionFileCnt = 0;
    public int selectFileCnt = 0;
    public LocalFilterType filter = null;
    public LocalOutputFormatType outputFormat = null;
            
    public static ContentVersionLink getContentVersionLink(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String baseManifestURL,
            Boolean presign,
            Boolean update,
            String filterS,
            String formatS,
            LoggerInf logger)
        throws TException
    {
        LocalFilterType filter = LocalFilterType.valueOf(filterS);
        LocalOutputFormatType outputFormat = LocalOutputFormatType.valueOf(formatS);
        return new ContentVersionLink(
                s3service, bucket, objectID, versionID, baseManifestURL, 
                presign, update, filter, outputFormat, logger);
    }    
    
    
            
    public static ContentVersionLink getContentVersionLink(
            ContentVersionLink.Request request,
            LoggerInf logger)
        throws TException
    {
        return new ContentVersionLink(
                request.s3service, request.bucket, request.objectID, request.versionID, request.linkBaseURL, 
                request.presign, request.update, request.filter, request.outputFormat, logger);
    }    
    
    public ContentVersionLink(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            int versionID,
            String baseManifestURL,
            Boolean presign,
            Boolean update,
            LocalFilterType filter,
            LocalOutputFormatType outputFormat,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        this.versionID = versionID;
        this.baseManifestURL = baseManifestURL;
        this.presign = presign;
        this.update = update;
        this.filter = filter;
        this.outputFormat = outputFormat;
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
            List<FileComponent> inComponents = map.getVersionComponents(versionID);
            currentVersion = map.getCurrent();
            localVersion = versionID;
            if (localVersion == 0) localVersion = currentVersion;
            if ((localVersion < 0) || (localVersion > currentVersion)) {
               throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                       + "input version invalid:"  
                       + " - passed version:" + versionID 
                       + " - current version:" + currentVersion);
            }
            List<FileComponent> fileComponents = setComponents(inComponents);
            versionFileCnt = inComponents.size();
            selectFileCnt = fileComponents.size();
            switch(outputFormat) {
                case invman:
                case storeman:
                    buildAddManifest(outManifest, fileComponents);
                    break;
                case json:
                    buildJSONManifest(outManifest, fileComponents);
                    break;
            }
            fileContent = FileContent.getFileContent(outManifest, logger);

            
        } catch(Exception ex) {
            setException(ex);
        }
    }  
    
    protected String removePrefix(String name)
    {
        if (name == null) return null;

        int inx = name.indexOf("/");
        if (inx >= 0) {
            return name.substring(inx+1);
        }
        return name;
    }
    
    public List<FileComponent> setComponents(List<FileComponent> components)
        throws TException
    {
        ArrayList<FileComponent> outComponents = new ArrayList<FileComponent>();
        
        try {
            for (FileComponent component : components) {
                FileComponent saveComponent = setComponent(component);
                if (saveComponent == null) continue;
                outComponents.add (component);
            }
            return outComponents;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }
           
    
    public FileComponent setComponent(FileComponent component)
        throws TException
    {
        try {
            String key = component.getLocalID();
            String newFilePath = component.getIdentifier();
            System.out.println("setComponent:"
                    + " - filter=" + filter
                    + " - key=" + key
                    + " - path=" + newFilePath
                    
            );
            switch(filter) {
                case producer: 
                    ArrayList <String> producerFilter = StorageConfig.getProducerFilter();
                    for (String filter : producerFilter) {
                        //System.out.println("producer - key=" + key + " - filter=" + filter);
                        if (key.contains(filter)) return null;
                    }
                    newFilePath = removePrefix(newFilePath);
                    break;
                    
                case nosystem:
                    if (key.contains("|system/")) {
                        return null;
                    } 
                    newFilePath = removePrefix(newFilePath);
                    break;
                    
                case none:
                    break;
            }
            if ((this.update) && (!key.contains("|" + versionID + "|"))) {
                return null;
            }
            URL fileLink = getFileLink(component);
            FileComponent saveComponent = component;
            saveComponent.setURL(fileLink);
            System.out.print(saveComponent.dump("contentIngestLink"));
            saveComponent.setIdentifier(newFilePath);
            return saveComponent;
            
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
            
            ManifestRowAbs.ManifestType manType = null;
            switch(outputFormat) {
                case invman:
                    manType = ManifestRowAbs.ManifestType.ingest;
                    break;
                case storeman:
                    manType = ManifestRowAbs.ManifestType.add;
                    break;
                default:
                    throw new TException.INVALID_OR_MISSING_PARM("outputFormat not supported:" 
                            + outputFormat.toString());
            }
            Manifest manOut = Manifest.getManifest(logger, manType);
            manOut.openOutput(outManifest);

            ManifestRowAdd manRowOut
                = (ManifestRowAdd)ManifestRowAbs.getManifestRow(ManifestRowAbs.ManifestType.add, logger);
            if (DEBUG) System.out.println("components size=" + components.size());
            for (FileComponent component : components) {
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
    
    /**
     * Build an add manifest from object manifest
     * @param versionID manifest version
     * @param outManifest manifest output file
     * @throws TException 
     */
    public void buildJSONManifest(File outManifest, List<FileComponent> components)
        throws TException
    {
        
            // #%columns | nfo:fileURL | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | nie:mimeType
        JSONObject jobj = null;
        try {
            if (DEBUG) {
                String mapXML = dumpVersionMap(map);
                System.out.println("\nXMLMAP=\n" + mapXML + "\n*****\n");
                
                System.out.println("\nMAP=\n" + map.dump("MAP HEADER"));
            }
            if (outManifest == null) {
                throw new TException.INVALID_OR_MISSING_PARM("outManifest required");
            }
            jobj = new JSONObject();
            JSONArray files = new JSONArray();
            
            if (DEBUG) System.out.println("components size=" + components.size());
            for (FileComponent component : components) {
                URL fileLink = component.getURL();
                JSONObject file = new JSONObject();
                files.put(file);
                file.put("fileURL", fileLink.toString());
                file.put("digestType", component.getMessageDigest().getJavaAlgorithm());
                file.put("digest", component.getMessageDigest().getValue());
                file.put("creationDate", component.getCreated().getIsoDate());    
                file.put("size", component.getSize());
                file.put("path", component.getIdentifier());   
                file.put("mimeType", component.getMimeType());
            }
    
            jobj.put("currentVersion", currentVersion);
            jobj.put("localVersion", localVersion);
            jobj.put("versionFileCnt", versionFileCnt);
            jobj.put("selectFileCnt", selectFileCnt);
            DateState date = new DateState();
            String dateS = date.getIsoDate();
            jobj.put("current", dateS);
            jobj.put("files", files);
            String jobjS = jobj.toString(2);
            FileUtil.string2File(outManifest, jobjS);
            
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        }
    }
    
    public URL getFileLink(FileComponent component)
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
                    component.getIdentifier());
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
    
    public static class Request
    {
        
        public Boolean presign = false;
        public Boolean update = false;
        public int versionID = -1;
        public Integer nodeID = null;
        public LocalFilterType filter = null;
        public LocalOutputFormatType outputFormat = null;
        public CloudStoreInf s3service = null;
        public String bucket = null;
        public Identifier objectID = null;
        public String linkBaseURL = null;

        public Request setPresign(String presignS)
            throws TException
        {
            if (presignS == null) return this;
            this.presign = setBoolean(presignS);
            if (this.presign == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "presign value not set:" + presignS);
            }
            return this;
        }

        public Request setUpdate(String updateS)
            throws TException
        {
            if (updateS == null) return this;
            this.update = setBoolean(updateS);
            if (this.update == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "update value not set:" + updateS);
            }
            return this;
        }


        public Request setVersionID(String versionIDS)
            throws TException
        {
            if (versionIDS == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "versionID value not supplied");
            }
            try {
                this.versionID = Integer.parseInt(versionIDS);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "versionID value invalid:" + versionIDS);
            }
            return this;
        }

        public Request setNodeID(Integer nodeID)
            throws TException
        {
            if (nodeID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "node value not supplied");
            }
            this.nodeID = nodeID;
            return this;
        }

        public Request setFilter(String filterS)
            throws TException
        {
            if (filterS == null) {
                this.filter = LocalFilterType.none;
                return this;
            }
            
            try {
                this.filter = LocalFilterType.valueOf(filterS);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "filter value invalid:" + filterS);
            }
            return this;
        }

        public Request setOutputFormat(String outputFormatS)
            throws TException
        {
            if (outputFormatS == null) {
                this.outputFormat = LocalOutputFormatType.invman;
                return this;
            }
            
            try {
                this.outputFormat = LocalOutputFormatType.valueOf(outputFormatS);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "filter value invalid:" + outputFormatS);
            }
            return this;
        }
        
        public Request setS3Service(CloudStoreInf s3service)
            throws TException
        {
            if (s3service == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "s3service value required");
            }
            this.s3service = s3service;
            return this;
        }
        
        public Request setBucket(String bucket)
            throws TException
        {
            if (bucket == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "bucket value required");
            }
            this.bucket = bucket;
            return this;
        }
        
        public Request setLinkBaseURL(String linkBaseURL)
            throws TException
        {
            if (linkBaseURL == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "baseManifestURL value required");
            }
            this.linkBaseURL = linkBaseURL;
            return this;
        }

        public Request setObjectID(String objectIDS)
            throws TException
        {
            if (objectIDS == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID value required");
            }
            
            try {
                this.objectID = new Identifier(objectIDS);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID value invalid:" + objectIDS);
            }
            
            return this;
        }

        public Boolean getPresign() {
            return presign;
        }
        
        public Boolean getUpdate() {
            return update;
        }

        public int getVersionID() {
            return versionID;
        }

        public LocalFilterType getFilter() {
            return filter;
        }

        public LocalOutputFormatType getOutputFormat() {
            return outputFormat;
        }

        public CloudStoreInf getS3service() {
            return s3service;
        }

        public String getBucket() {
            return bucket;
        }

        public Identifier getObjectID() {
            return objectID;
        }

        public String getLinkBaseURL() {
            return linkBaseURL;
        }

        public Integer getNodeID() {
            return nodeID;
        }
    
        public String dump(String header)
        {
            StringBuffer buf = new StringBuffer();
            buf.append(header + "\n");
            buf.append(" - presign:" + presign + "\n");
            buf.append(" - update:" + update + "\n");
            buf.append(" - outputFormat:" + outputFormat + "\n");
            buf.append(" - filter:" + filter + "\n");
            buf.append(" - nodeID:" + nodeID + "\n");
            buf.append(" - objectID:" + objectID.getValue() + "\n");
            buf.append(" - versionID:" + versionID + "\n");
            buf.append(" - bucket:" + bucket + "\n");
            buf.append(" - linkBaseURL:" + linkBaseURL + "\n");
            return buf.toString();
        }
    }
}
