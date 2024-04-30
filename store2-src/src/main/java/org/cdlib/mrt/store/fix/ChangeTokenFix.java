
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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.VersionMapUtil;
import org.cdlib.mrt.cloud.action.CloudActionAbs;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.store.KeyFileInf;
import org.cdlib.mrt.utility.ArchiveBuilderBase;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.FixityTests;
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
public class ChangeTokenFix
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
    protected ArrayList<LinkedHashMap<String, FileComponent>> outComponents = new ArrayList<LinkedHashMap<String, FileComponent>>();
    protected ArrayList<AddVersionComponent> addVersionComponents = new ArrayList<>();
    protected LinkedHashMap<String, FileComponent> deleteComponents = new LinkedHashMap<String, FileComponent>();
    protected VersionMap outVersionMap = null;
    protected long totalDeleteBytes = 0;
    protected long totalSaveBytes = 0;
    protected int versionDuplicateCnt = 0;
    protected int deleteCnt = 0;
    protected int duplicateDeleteCnt = 0;
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
    
                    
    public ChangeTokenFix(
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
            logFix.info("Start process\n"
                    + " - nodeID=" + this.nodeID
                    + " - objectID="+ this.objectID.getValue() + "\n"
                    + " - bucket=" + bucket + "\n"
                    + " - objectCurrent=" + this.objectCurrent + "\n"
            );
            for (int verid=1; verid <= objectCurrent; verid++) {
                addVersion(verid);
            }
            addSaved();
            replaceMap();
            verifyDelete();
            deleteRemoved();
            
            dumpCounts();
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    protected void addVersion(int versionID)
        throws TException
    { 
        LinkedHashMap<String, FileComponent>  newVersion = new LinkedHashMap<String, FileComponent>();
        try {
            List<FileComponent> currentVersion = getVersion(versionID);
            for (FileComponent currentComponent : currentVersion) {
                processComponent(versionID, currentComponent, newVersion);
            }
            outComponents.add(newVersion);
            int failCnt = verifyVersion(currentVersion, newVersion);
            if (failCnt != 0) {
                throw new TException.INVALID_ARCHITECTURE("Bad output version:" + failCnt);
            } else {
                log4j.debug("Version verified:" + versionID);
            }
            
            //dumpVersion(versionID, newVersion);
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    
    protected int verifyVersion(
            List<FileComponent> currentVersion,
            LinkedHashMap<String, FileComponent>  newVersionHash)
       throws TException
    {
        try {
            //LinkedHashMap<String, FileComponent> currentHash = getHashVersionDigests(currentVersion);
            LinkedHashMap<String, ArrayList<FileComponent>> newComponentHash = getHashVersionDigests(newVersionHash);
            int failcnt = 0;
            for (FileComponent currentComponent : currentVersion) {
                boolean match = matchComponent(currentComponent, newComponentHash);
                if (!match) {
                    failcnt++;
                }
            }
            return failcnt;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    }    
    protected void addDeleteComponent(FileComponent deleteComponent)
       throws TException
    {
        try {
            String deleteKey = deleteComponent.getLocalID();
            FileComponent testDeleteComponent = deleteComponents.get(deleteKey);
            if (testDeleteComponent != null) {
                duplicateDeleteCnt++;
                return;
            }
            deleteComponents.put(deleteKey, deleteComponent);
            deleteCnt++;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    }
    
    
    protected void verifyDelete()
       throws TException
    {
        try {
            Set<String> keys = deleteComponents.keySet();
            for (String key : keys) {
                FileComponent deleteComponent = deleteComponents.get(key);
                String deleteKey = deleteComponent.getLocalID();
                if (!deleteKey.contains(MATCH_TOKEN + "=")) {
                    throw new TException.INVALID_DATA_FORMAT("Invalid delete key:" + deleteKey);
                }
            }
            log4j.debug("Delete verified");
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    }
    
    protected LinkedHashMap<String, ArrayList<FileComponent>> getHashVersionDigests(
            LinkedHashMap<String, FileComponent>  hashFileID)
    {
        LinkedHashMap<String, ArrayList<FileComponent>> hashVersionDigests = new LinkedHashMap<>();
        Set<String> fileIDs = hashFileID.keySet();
        for (String fileID: fileIDs) {
            FileComponent component = hashFileID.get(fileID);
            MessageDigest digest = component.getMessageDigest();
            String digestS = digest.getValue();
            ArrayList<FileComponent> components = hashVersionDigests.get(digestS);
            if (components == null) {
                components = new ArrayList<>();
            }
            components.add(component);
            hashVersionDigests.put(digestS, components);
        }
        return hashVersionDigests;
    }
    
    
    
    protected boolean matchComponent(
            FileComponent currentComponent, LinkedHashMap<String, ArrayList<FileComponent>> newComponentHash)
    {
        MessageDigest digest = currentComponent.getMessageDigest();
        String currentDigestS = digest.getValue();
        ArrayList<FileComponent> newComponents = newComponentHash.get(currentDigestS);
        if (newComponents == null) {
            log4j.debug(">>>FAIL CurrentDigest missing:" + currentDigestS);
            return false;
        }
        String currentFileID = currentComponent.getIdentifier();
        for (FileComponent testComponent : newComponents) {
            String newFileID = testComponent.getIdentifier();
            if (currentFileID.contains(newFileID)) {
                log4j.trace("Match:\n"
                        + " - digest:" + currentDigestS + "\n"
                        + " - current:" + currentFileID + "\n"
                        + " - new:" + newFileID + "\n"
                );
                return true;
            }
        }
        log4j.debug(currentComponent.dump(">>>NO Match"));
        for (FileComponent testComponent : newComponents) {
            log4j.debug(testComponent.dump(" - fail:"));
        }
        return false;
    }
          
                
    
    protected void processComponent(int versionID, 
            FileComponent currentComponent, 
            LinkedHashMap<String, FileComponent>  newVersion)
        throws TException
    { 
        String fileID = currentComponent.getIdentifier();
        String originalID = fileID;
        inComponentCnt++;
        try {
          
            if (!fileID.contains(TEST_TOKEN)) {
                newVersion.put(fileID, currentComponent);
                noChangeCnt++;
                log4j.trace(">>>no TEST_TOKEN(" + versionID + ")=" + fileID);
                return;
            }
            
            // if fixed fileID results is unchanged then no further action required
            String extractFileID = fixToken(fileID);
            if (extractFileID.equals(fileID)) {
                newVersion.put(fileID, currentComponent);
                noChangeCnt++;
                System.out.println(">>>equalID(" + versionID + ")=" + fileID);
                return;
            }
            
            // key added to delete list
            addDeleteComponent(currentComponent);
            
            // ERROR situation where a fixed fileID already exists in version
            // fileID must be unique to version
            // no processing on corrected fileID - duplicate dropped
            fileID = extractFileID;
            FileComponent checkComponent = newVersion.get(fileID);
            if (checkComponent != null) {
                versionDuplicateCnt++;
                log4j.warn("Duplicate detected:"
                        + " - objectID:" + objectID.getValue()
                        + " - versionID:" + versionID
                        + " - fileID:" + fileID
                );
                duplicateCnt++;
                System.out.println(">>>skip version contains multiple(" + versionID + ")=" + fileID);
                return;
            }
            log4j.trace(">>>orgID(" + versionID + ")=" + originalID + "\n"
                    + ">>>modID(" + versionID + ")=" + fileID 
            );
            
            // Test new key (+content) needs to be added
            FileComponent addComponent = setNewComponent(versionID, fileID, currentComponent);
            log4j.trace(">>>mcKey(" + versionID + ")=" + addComponent.getLocalID()+ "\n");
            newVersion.put(fileID, addComponent);
            changeCnt++;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
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
            for (AddVersionComponent addVersionComponent : addVersionComponents) {
                saveFileComponent(addVersionComponent);
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
                log4j.debug("deleteRemove key(" + deleteSize + ")=" + deleteKey);
                if (runS3) {
                    CloudResponse response = s3service.deleteObject(bucket, deleteKey);
                }
                totalDeleteBytes += deleteSize;
                deleteCnt++;
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
            LinkedHashMap<String, FileComponent> componentHash = outComponents.get(verinx);
            int tokenCnt = 0;
            Set<String> keys = componentHash.keySet(); 
            log4j.debug("!!!getNewVersionList(" + versionID + ") keys=" + keys.size());
            for (String key : keys) {
                if (key.contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN key found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                }
                FileComponent extComponent = componentHash.get(key);
                if (extComponent.getIdentifier().contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN identifier found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                }
                if (extComponent.getLocalID().contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN localID found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                }
                components.add(extComponent);
            }
            System.out.println("!!!getNewVersionList(" + versionID + ") tokenCnt=" + tokenCnt);
            outComponentCnt += components.size();
            return components;
           
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    protected void  addNewVersionList(LinkedHashMap<String, FileComponent> componentHash)
        throws TException
    { 
        
        ArrayList<FileComponent> components = new ArrayList<>();
        System.out.println("!!!addNewVersionList"
                + " - componentHash.size:" + componentHash.size()
        );
                
        try {
            int tokenCnt = 0;
            Set<String> keys = componentHash.keySet(); 
            for (String key : keys) {
                if (key.contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN key found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                }
                FileComponent extComponent = componentHash.get(key);
                if (extComponent.getIdentifier().contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN identifier found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                }
                if (extComponent.getLocalID().contains(TEST_TOKEN)) {
                    log4j.warn("!!!TEST_TOKEN localID found - Version(" + versionID + "):" + key);
                    tokenCnt++;
                }
                components.add(extComponent);
            }
            System.out.println("!!!getNewVersionList(" + versionID + ") tokenCnt=" + tokenCnt);
            outVersionMap.addVersion(components);
           
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    protected static  String fixToken(String oldFileID)
        throws TException
    {
        return removeQueryProp(MATCH_TOKEN, oldFileID);
    }
    
    protected FileComponent setNewComponent(int versionID, String fileID, FileComponent currentComponent)
        throws TException
    {
        try {
            FileComponent newComponent = new FileComponent();
            newComponent.copy(currentComponent);
            newComponent.setIdentifier(fileID);
            String oldLocalID = currentComponent.getLocalID();
            String newLocalID = objectID.getValue() + "|" + versionID + "|" + fileID;
            if (versionID == 1) {
                newComponent.setLocalID(newLocalID);
                addFileComponent(versionID, oldLocalID, newComponent);
                return newComponent;
            }
            
            // match this component against previous versions for setting local key pointer
            for (LinkedHashMap<String, FileComponent> verComponents : outComponents) {
                FileComponent matchComponent = verComponents.get(fileID);
                
                // no match this version
                if (matchComponent == null) continue;
                // see if real match by comparing length/digest
                if (versionMap.isMatch(matchComponent, newComponent)) {
                    newComponent.setLocalID(matchComponent.getLocalID());
                    matchKeyCnt++;
                    
                logFix.info("matchFileComponent:" 
                        + " - ark=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + "\n"
                        + " - matchKey:" + matchComponent.getLocalID() + "\n"
                );
                    return newComponent;
                }
            }
            
            // no earlier version matches this fileID - do addFile processing
            
            newComponent.setLocalID(newLocalID);
            addFileComponent(versionID, oldLocalID, newComponent);
            return newComponent;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    /**
     * Add to list to be added later
     * @param versionID current version for component
     * @param currentComponent - component definition
     * @throws TException 
     */
    protected void addFileComponent(int versionID, String oldLocalID, FileComponent currentComponent)
        throws TException
    {
        newKeyCnt++;
        AddVersionComponent addVersionComponent = AddVersionComponent.get(versionID, oldLocalID, currentComponent);
        addVersionComponents.add(addVersionComponent);
    }
    
    /**
     * Do S3 write of component using new fileID (without changeToken
     * @param versionID current version for component
     * @param currentComponent - component definition
     * @throws TException 
     */
    protected void saveFileComponent(AddVersionComponent addVersionComponent)
        throws TException
    {
        int versionID = addVersionComponent.versionID;
        FileComponent currentComponent = addVersionComponent.addComponent;
        String newS3Key = currentComponent.getLocalID();
        String oldS3Key = addVersionComponent.oldS3Key;
        logFix.info("saveFileComponent:\n"
                + " - newS3Key:" + newS3Key + "\n"
                + " - oldS3Key:" + oldS3Key + "\n"
        );
        File tmpFile = FileUtil.getTempFile("extract", ".txt");
        try {
            CloudResponse response = new CloudResponse(bucket, oldS3Key);
            s3service.getObject(bucket, oldS3Key, tmpFile, response);
            currentComponent.setComponentFile(tmpFile);
            currentComponent.setLocalID(newS3Key);
            String oldMimeType = currentComponent.getMimeType();
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
        if (runS3) {
            s3service.putObject(bucket, newS3Key, tmpFile);
        }
        log4j.debug("Add S3(" + bucket + "):" + newS3Key);
        saveFileCnt++;
        totalSaveBytes += tmpFile.length();
            
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

    protected void dumpCountsOld()
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
            //LinkedHashMap<String, Object> request = new LinkedHashMap<>();
            
            
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("objectID", objectID.getValue());
            jsonRequest.put("node", "" + nodeID);
            //LinkedHashMap<String, Object> counts = new LinkedHashMap<>();
            JSONObject jsonCounts = new JSONObject();
            jsonCounts.put("inComponent", inComponentCnt);
            jsonCounts.put("outComponent", outComponentCnt);
            jsonCounts.put("noChange", noChangeCnt);
            jsonCounts.put("change", changeCnt);
            jsonCounts.put("saveFileCnt", saveFileCnt);
            jsonCounts.put("totalSaveBytes", totalSaveBytes);
            jsonCounts.put("duplicateDeleteCnt", duplicateDeleteCnt);
            jsonCounts.put("deleteComponents", deleteComponents.size());
            jsonCounts.put("totalDeleteBytes", totalDeleteBytes);
            jsonCounts.put("duplicateKeys", duplicateCnt);
            jsonCounts.put("matchKey", matchKeyCnt);
            jsonCounts.put("newKey", newKeyCnt);
            jsonCounts.put("mimeChange", mimeChangeCnt);
            jsonCounts.put("mimeMatch", mimeMatchCnt);
            //LogManager.getLogger().info(counts);
            
            //JSONObject jsonRequest = new JSONObject(request);
            //JSONObject jsonCounts = new JSONObject(counts);
            jsonResponse = new JSONObject();
            jsonResponse.put("status", "ok");
            jsonResponse.put("request", jsonRequest);
            jsonResponse.put("counts", jsonCounts);
            System.out.println(jsonResponse.toString(2));
            log4j.info(jsonResponse);
            
    }

    public JSONObject getJsonResponse() {
        return jsonResponse;
    }
   
   protected static class AddVersionComponent
   {
       public static AddVersionComponent get(int versionID, String oldS3Key, FileComponent addComponent)
       {
           return new AddVersionComponent(versionID, oldS3Key, addComponent);
       }
       public int versionID = 0;
       public String oldS3Key = null;
       public FileComponent addComponent = null;
       public AddVersionComponent(int versionID, String oldS3Key, FileComponent addComponent) 
       {
           this.versionID = versionID;
           this.oldS3Key = oldS3Key;
           this.addComponent = addComponent;
       }
   }
}
