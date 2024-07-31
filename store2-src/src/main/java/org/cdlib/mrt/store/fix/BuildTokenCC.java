
package org.cdlib.mrt.store.fix;

/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.json.JSONObject;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on
 * Amazon S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 *                   AwsCredentials.properties file before you try to run this
 *                   sample.
 * http://aws.amazon.com/security-credentials
 */
public class BuildTokenCC
        extends CloudFixAbs
{
    
    protected static final String NAME = "ChangeTokenFix";
    protected static final String MESSAGE = NAME + ": ";
    private boolean DEBUG = false;
    protected static String MATCH_TOKEN = "changeToken"; 
    protected static String TEST_TOKEN = MATCH_TOKEN + "="; 
    protected Tika tika = null;
    protected String FAIL = "0d104d06-8b15-46e4-a00a-83fce4daf1c2/files:files/161/file/UCCE_HUM_027_001_001_0162.txt";
    protected JSONObject jsonResponse = null;
    protected boolean runS3 = false;
    
   
    
    protected int version = 0;
    protected CloudList fullCloudList = null;
    protected ChangeTokenCC changeTokenCC = null;
    protected ArrayList<LinkedHashMap<String, ChangeComponent>> outComponents = null;
    protected LinkedHashMap<String, ChangeComponent> addComponents = new LinkedHashMap<>();
    protected LinkedHashMap<String, FileComponent> deleteComponents = new LinkedHashMap<String, FileComponent>();
    protected VersionMap outVersionMap = null;
    protected long totalDeleteBytes = 0;
    protected long totalSaveBytes = 0;
    protected int versionDuplicateCnt = 0;
    protected int deleteCnt = 0;
    protected int duplicateDeleteCnt = 0;
    protected int duplicateAddCnt = 0;
    protected int changeCnt = 0;
    protected int noChangeCnt = 0;
    protected int duplicateCnt = 0;
    protected int inComponentCnt = 0;
    protected int outComponentCnt = 0;
    protected int matchKeyCnt = 0;
    protected int newKeyCnt = 0;
    protected int saveFileCnt = 0;
    protected int mimeChangeCnt = 0;
    protected int mimeMatchCnt = 0;
    
                    
    public BuildTokenCC(
            NodeIO nodeIO, 
            Long nodeID, 
            Identifier objectID, 
            LoggerInf logger)
        throws TException
    {
        super(nodeIO, nodeID, objectID, logger);
        outVersionMap = new VersionMap(objectID, logger);
        tika = new Tika(logger);
        log4j.info(MESSAGE
                + " - nodeID=" + this.nodeID
                + " - objectID="+ this.objectID.getValue()
                + " - bucket=" + bucket
                + " - objectCurrent=" + this.objectCurrent
        );
    }
    
    public void process()
       throws TException
    {
        try {
            buildOutChangeComponents();
            replaceMap();
            buildAddDeleteComponents();
            addSaved();
            deleteRemoved();
            dumpOldMap();
            dumpCounts();
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    protected void buildOutChangeComponents()
        throws TException
    {
        
            changeTokenCC = new ChangeTokenCC(
                nodeIO, 
                nodeID, 
                objectID, 
                logger);
            changeTokenCC.process();
            outComponents = changeTokenCC.getOutComponents();
            setCCCnts();
    }
    
    protected void buildAddDeleteComponents()
        throws TException
    {
        for (LinkedHashMap<String, ChangeComponent> verCC : outComponents) {
            Set<String> verKeys = verCC.keySet();
            for (String key : verKeys) {
                ChangeComponent component = verCC.get(key);
                FileComponent inComponent = component.getInComponent();
                FileComponent outComponent = component.getOutComponent();
                logFix.info("ChangeComponent - version=" + component.getVersionID() + " - action:" + component.op.toString() + "\n"
                        + "in.fileID: " + inComponent.getIdentifier() + "\n"
                        + "out.fileID:" + outComponent.getIdentifier() + "\n"
                        + "in.Key: " + inComponent.getLocalID() + "\n"
                        + "out.Key:" + outComponent.getLocalID() + "\n"
                );
                saveAddComponent(component);
                saveDeleteComponent(component);
            }
            
        }
    }
    
    protected void saveDeleteComponent(ChangeComponent component)
        throws TException
    {
        if ((component.getOp() != ChangeComponent.Operation.move_data) 
                && (component.getOp() != ChangeComponent.Operation.move_reference))
                return;
        
        FileComponent deleteComponent = component.getInComponent();
        String deleteKey = deleteComponent.getLocalID();
        if (!deleteKey.contains(MATCH_TOKEN + "=")) {
            throw new TException.INVALID_DATA_FORMAT("Invalid delete key:" + deleteKey);
        }
        FileComponent existing = deleteComponents.get(deleteKey);
        if (existing != null) {
            duplicateDeleteCnt++;
        } else {
            deleteComponents.put(deleteKey,deleteComponent);
        }
        log4j.debug("Delete(" + component.getVersionID() + "):" + deleteKey);
    }
    
    protected void saveAddComponent(ChangeComponent component)
        throws TException
    {
        if (component.getOp() == ChangeComponent.Operation.provenance) {}
        else if (component.getOp() != ChangeComponent.Operation.move_data) return;
        FileComponent addComponent = component.getOutComponent();
        String addKey = addComponent.getLocalID();
        if (addKey.contains(MATCH_TOKEN + "=")) {
            throw new TException.INVALID_DATA_FORMAT("Invalid add key:" + addKey);
        }
        ChangeComponent existing = addComponents.get(addKey);
        if (existing != null) {
            duplicateAddCnt++;
        } else {
            addComponents.put(addKey,component);
        }
        log4j.debug("Add(" + component.getVersionID() + "):\n"
                + " - addFileID:" + addComponent.getIdentifier()   + "\n"
                + " - addKey:" + addKey + "\n"
        );
    }
    
    
    protected void replaceMap()
       throws TException
    {
        
        File manifestFile = null;
        try {
            for (int verid=1; verid <= objectCurrent; verid++) {
                List<FileComponent> addList = getNewVersionList(verid);
                outVersionMap.addVersion(addList);
            }
            if (false) return;
            manifestFile = buildXMLManifest (outVersionMap, null);
            String manifestFileContent = FileUtil.file2String(manifestFile);
            log4j.debug("***replaceMap - manifestFile:\n" + manifestFileContent);
            if (runS3) {
                s3service.putManifest(bucket, objectID, manifestFile);
            }
            
        } catch (TException ex) {
            ex.printStackTrace();
            throw ex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            manifestFile.delete();
        }
        
    }
    protected void addSaved()
       throws TException
    {
        
        try {
            Set<String> addKeys = addComponents.keySet();
            for (String addKey : addKeys) {
                ChangeComponent addcomponent = addComponents.get(addKey);
                saveFileComponent(addcomponent);
            }
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    protected void deleteRemoved()
       throws TException
    {
        
        try {
            Set<String> keys = deleteComponents.keySet();
            for (String key : keys) {
                FileComponent deleteComponent = deleteComponents.get(key);
                String deleteKey = deleteComponent.getLocalID();
                long deleteSize = deleteComponent.getSize();
                //logFix.info("deleteRemove S3(" + deleteKey + ") size=" + deleteSize);
                if (runS3) {
                    CloudResponse response = s3service.deleteObject(bucket, deleteKey);
                }
                totalDeleteBytes += deleteSize;
                deleteCnt++;
                logFixMsg(runS3, "DELETE", bucket, key, "", deleteSize);
            }
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    protected List<FileComponent> getVersion(int versionID)
       throws TException
    {
        List<FileComponent> version = versionMap.getVersionComponents(versionID);
        return version;
    }
    
    protected List<FileComponent>  getNewVersionList(int versionID)
        throws TException
    { 
        
        ArrayList<FileComponent> components = new ArrayList<>();
        System.out.println("!!!getNewVersionList=" + versionID 
                + " - outComponents.size:" + outComponents.size()
        );
                
        try {
            int verinx = versionID - 1;
            LinkedHashMap<String, ChangeComponent> componentHash = outComponents.get(verinx);
            int tokenCnt = 0;
            Set<String> keys = componentHash.keySet(); 
            log4j.debug("!!!getNewVersionList(" + versionID + ") keys=" + keys.size());
            for (String key : keys) {
                if (key.contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN key found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                }
                ChangeComponent changeComponent = componentHash.get(key);
                FileComponent extComponent = changeComponent.getOutComponent();
                if (extComponent.getIdentifier().contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN identifier found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                    throw new TException.INVALID_CONFIGURATION("!!!TEST_TOKEN identifier found - Version(" + versionID + "):" + key);
                }
                components.add(extComponent);
            }
            System.out.println("!!!getNewVersionList(" + versionID + ") tokenCnt=" + tokenCnt);
            //outComponentCnt = components.size();
            return components;
           
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    /**
     * Do S3 write of component using new fileID (without changeToken
     * @param versionID current version for component
     * @param currentComponent - component definition
     * @throws TException 
     */
    protected void saveFileComponent(ChangeComponent addcomponent)
        throws TException
    {
        FileComponent inComponent = addcomponent.getInComponent();
        FileComponent outComponent = addcomponent.getOutComponent();
        String newS3Key = outComponent.getLocalID();
        String oldS3Key = inComponent.getLocalID();
        long inLength = inComponent.getSize();
        logFix.debug("ChangeComponent:\n"
                + " - newS3Key:" + newS3Key + "\n"
                + " - oldS3Key:" + oldS3Key + "\n"
                + " - inLength:" + inLength + "\n"
        );
        File tmpFile = FileUtil.getTempFile("extract", ".txt");
        try {
            if (runS3) {
                CloudResponse response = new CloudResponse(bucket, oldS3Key);
                s3service.getObject(bucket, oldS3Key, tmpFile, response);
                if (inLength != tmpFile.length()) {
                    throw new TException.INVALID_DATA_FORMAT("s3 length does not match extract length"
                        + " - newS3Key:" + newS3Key
                        + " - oldS3Key:" + oldS3Key
                        + " - inLength:" + inLength
                        + " - tmpFile.length():" + tmpFile.length()
                    );
                }
                String oldMimeType = outComponent.getMimeType();
                String newMimeType = getMime(tmpFile, newS3Key);
                if (!oldMimeType.equals(newMimeType)) {
                    log4j.trace("!!!changeMime\n"
                        + " - newS3Key:" + newS3Key + "\n"
                        + " - oldS3Key:" + oldS3Key + "\n"
                        + " - oldMimeType:" + oldMimeType + "\n"
                        + " - newMimeType:" + newMimeType + "\n"
                    );
                    mimeChangeCnt++;
                } else {
                    log4j.trace("!!!mimeMatch\n"
                        + " - newS3Key:" + newS3Key + "\n"
                        + " - oldS3Key:" + oldS3Key + "\n"
                        + " - mimeType:" + oldMimeType + "\n"
                    );
                    mimeMatchCnt++;
                }
                if (runS3) s3service.putObject(bucket, newS3Key, tmpFile);
            }
            
            logFixMsg(runS3, "ADD", bucket, oldS3Key, newS3Key, inLength);
            //logFix.info("Add S3(" + newS3Key + ") size=" + inLength);
            //log4j.debug("Add S3(" + bucket + "):" + newS3Key);
            saveFileCnt++;
            totalSaveBytes += inLength;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
            tmpFile.delete();
        }
    }
    
    protected String getMime(File tmpFile, String fileID)
        throws TException
    {

        String mimeType = null;
        InputStream componentStream = null;
        try {
            componentStream = new FileInputStream(tmpFile);
            mimeType = tika.getMimeType(componentStream, fileID);
            if (DEBUG) System.out.println("add mimeType=" + mimeType);
            return mimeType;

        }  catch (Exception ex) {
            System.out.println("WARNING tika exception:" + ex);
            return null;

        } finally {
            try {
                if (componentStream != null) {
                    componentStream.close();
                }
            } catch (Exception ex) { }
            
        }
    }
    
   protected void dumpVersion(int versionID, LinkedHashMap<String, FileComponent>  newVersion)
   {
       Set<String> keys = newVersion.keySet();
       for (String key : keys) {
           System.out.println("@@@DUMP(" + versionID + "):" + key);
           
       }
    }
   
    protected void dumpOldMap()
        throws TException
    {
        System.out.println("******* OLD MAP >>>>>>>>");
        String oldMapS = FileUtil.file2String(oldManFile);
        System.out.println(oldMapS);
        System.out.println("<<<<<<< OLD MAP ********");
    }

    protected void dumpCounts()
    {
       
            /*
            log4j.info("NoChange count=" + noChangeCnt);
            log4j.info("Change count=" + changeCnt);
            log4j.info("Delete count=" + deleteComponents.size());
            log4j.info("Duplicate count=" + duplicateCnt);
            log4j.info("InComponent count=" + inComponentCnt);
            log4j.info("OutComponent count=" + inComponentCnt);
            log4j.info("Match Key count=" + matchKeyCnt);
            log4j.info("New Key count=" + newKeyCnt);
            */
            LinkedHashMap<String, Object> count = new LinkedHashMap<>();
            
            
            //JSONObject jsonRequest = new JSONObject();
            count.put("run", runS3);
            count.put("status", "ok");
            count.put("objectID", objectID.getValue());
            count.put("node", "" + nodeID);
            //LinkedHashMap<String, Object> counts = new LinkedHashMap<>();
            //JSONObject jsonCounts = new JSONObject();
            count.put("inComponent", inComponentCnt);
            count.put("outComponent", outComponentCnt);
            count.put("noChange", noChangeCnt);
            count.put("change", changeCnt);
            count.put("saveFileCnt", saveFileCnt);
            count.put("totalSaveBytes", totalSaveBytes);
            count.put("duplicateDeleteCnt", duplicateDeleteCnt);
            count.put("addComponents", addComponents.size());
            count.put("deleteComponents", deleteComponents.size());
            count.put("totalDeleteBytes", totalDeleteBytes);
            count.put("duplicateKeys", duplicateCnt);
            count.put("matchKey", matchKeyCnt);
            count.put("newKey", newKeyCnt);
            count.put("mimeChange", mimeChangeCnt);
            count.put("mimeMatch", mimeMatchCnt);
            //LogManager.getLogger().info(counts);
            
            //JSONObject jsonRequest = new JSONObject(request);
            //JSONObject jsonCounts = new JSONObject(counts);
            log4j.info(count);
            
            jsonResponse = new JSONObject(count);
            logFix.info(jsonResponse.toString(2));
    }
    
    protected void setCCCnts()
    {
        deleteCnt = changeTokenCC.getDeleteCnt();
        changeCnt = changeTokenCC.getChangeCnt();
        noChangeCnt = changeTokenCC.getNoChangeCnt();
        duplicateCnt = changeTokenCC.getDuplicateCnt();
        inComponentCnt = changeTokenCC.getInComponentCnt();
        outComponentCnt = changeTokenCC.getOutComponentCnt();
        matchKeyCnt = changeTokenCC.getMatchKeyCnt();
    }
    

    public JSONObject getJsonResponse() {
        return jsonResponse;
    }
}
