
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.Level;

import org.json.JSONObject;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.log.utility.AddStateEntryGen;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.store.action.MatchObject;
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
    protected String collection = null;
    protected CloudList fullCloudList = null;
    protected ChangeTokenCC changeTokenCC = null;
    protected ArrayList<LinkedHashMap<String, ChangeComponent>> outComponents = null;
    protected LinkedHashMap<String, ChangeComponent> addComponents = new LinkedHashMap<>();
    protected LinkedHashMap<String, FileComponent> deleteComponents = new LinkedHashMap<String, FileComponent>();
    protected LinkedHashMap<ChangeComponent.Operation, Integer> tallyOperation = new LinkedHashMap<>();
    protected VersionMap outVersionMap = null;
    protected MatchObject matchObject = null;
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
    protected int saveFileCnt = 0;
    protected int mimeChangeCnt = 0;
    protected int mimeMatchCnt = 0;
    protected JSONObject jsonCounts = null;
    protected boolean newManifestWritten = false;
    protected int runAdd = 0;
    protected int runProvenance = 0;
    protected int runDelete = 0;
    
                    
    public BuildTokenCC(
            String collection,
            NodeIO nodeIO, 
            Long nodeID, 
            Identifier objectID, 
            Boolean runS3,
            LoggerInf logger)
        throws TException
    {
        super(nodeIO, nodeID, objectID, logger);
        if (runS3 != null) {
            this.runS3 = runS3;
            log4j.info("runS3:" + this.runS3);
            //this.runS3 = false; //!!!!! do not remove until ready
        }
        this.collection = collection;
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
            //dumpOldMap();
            dumpMap("OLD", oldManFile, true, true);
            //dumpCounts();
            logCounts();
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
                tallyComponent(component);
                saveAddComponent(component);
                saveDeleteComponent(component);
            }
            
        }
    }
    
    protected void tallyComponent(ChangeComponent component)
        throws TException
    {
        Integer cnt = tallyOperation.get(component.op);
        if (cnt == null) cnt = 0;
        cnt++;
        tallyOperation.put(component.op, cnt);
    }
    
    protected JSONObject tallyComponent2JSON()
        throws TException
    {
        JSONObject tallyJSON = new JSONObject();
        Set<ChangeComponent.Operation> keys = tallyOperation.keySet();
        for (ChangeComponent.Operation op : keys) {
            Integer opTally = tallyOperation.get(op);
            tallyJSON.put("sum-" + op.toString(), opTally);
        }
        return tallyJSON;
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
            //String manifestFileContent = FileUtil.file2String(manifestFile);
            boolean isMatch = runMatchObject();
            log4j.info("***runMatchObject:" + isMatch);
            if (!isMatch) {
                throw new TException.INVALID_CONFIGURATION("Expected new Version Map invalid");
            }
            dumpMap("replaceMap - manifestFile", manifestFile, true, true);
            if (runS3) {
                s3service.putManifest(bucket, objectID, manifestFile);
                newManifestWritten = true;
                log4j.info("RUNS3 add new manifest:"
                        + " - bucket:" + bucket
                        + " - objectID:" + objectID.getValue()
                );
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
    
    protected boolean runMatchObject()
        throws TException
    {
        
        matchObject = new MatchObject(versionMap, nodeID, objectID, 
            outVersionMap, nodeID, objectID, logger);
        matchObject.matchDigest();

        int digestFailCnt = matchObject.getDiffCnt("digest_fail");

        //matchObject.printStatus(); //!!!
        log4j.debug("digestFailCnt:" + digestFailCnt);
        if (digestFailCnt != 1) {
            log4j.warn("FAIL digestFailCnt:" + digestFailCnt);
            return false;
        }

        List<MatchObject.DiffInfo> diffs = matchObject.getDiffList("digest_fail");
        MatchObject.DiffInfo diff = diffs.get(0);
        String fileID = diff.fileComponent.getIdentifier();
        log4j.info("New fileID:" + fileID);
        if (fileID.equals("system/provenance_manifest.xml")) {
            return true;
        } 
        return false;
            
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
                    runDelete++;
                    log4j.info("RUNS3 delete file:"
                            + " - bucket:" + bucket
                            + " - deleteKey:" + deleteKey
                    );
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
        log4j.debug("!!!getNewVersionList=" + versionID 
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
            log4j.debug("!!!getNewVersionList(" + versionID + ") tokenCnt=" + tokenCnt);
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
            if (newS3Key.contains("system/provenance_manifest.xml")) {
                saveProvenance(addcomponent);
                return;
            }
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
                if ((oldMimeType == null) || (newMimeType == null)) {
                    log4j.debug("mimeType null:"
                            + " - newS3Key:" + newS3Key + " - newMimeType:" + newMimeType
                            + " - oldS3Key:" + oldS3Key + " - oldMimeType:" + oldMimeType
                    );

                } else if (!oldMimeType.equals(newMimeType)) {
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

                CloudResponse cloudResponse = s3service.putObject(bucket, newS3Key, tmpFile);
                if (response.getStatus() == CloudResponse.ResponseStatus.fail) {
                    throw new TException.GENERAL_EXCEPTION("BuildTokenCC.saveProvenance" + response.getErrMsg());
                }
                
                runAdd++;
            }
            
            logFixMsg(runS3, "ADD", bucket, oldS3Key, newS3Key, inLength);
            //logFix.info("Add S3(" + newS3Key + ") size=" + inLength);
            //log4j.debug("Add S3(" + bucket + "):" + newS3Key);
            saveFileCnt++;
            totalSaveBytes += inLength;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            tmpFile.delete();
        }
    }
    
    protected void saveProvenance(ChangeComponent addcomponent)
        throws TException
    {
        FileComponent outComponent = addcomponent.getOutComponent();
        
        String newS3Key = outComponent.getLocalID();
        logFix.debug("saveProvenance - ChangeComponent:\n"
                + " - newS3Key:" + newS3Key + "\n"
        );
        //dumpManFile("saveProvenance2", oldManFile);
        
        try {
            if (runS3) {
                CloudResponse response = s3service.putObject(bucket, newS3Key, oldManFile);
                if (response.getStatus() == CloudResponse.ResponseStatus.fail) {
                    throw new TException.GENERAL_EXCEPTION("BuildTokenCC.saveProvenance" + response.getErrMsg());
                }
                runProvenance++;
            }
            
            logFixMsg(runS3, "ADD", bucket, "NEW FILE", newS3Key, oldManFile.length());
            //logFix.info("Add S3(" + newS3Key + ") size=" + inLength);
            //log4j.debug("Add S3(" + bucket + "):" + newS3Key);
            saveFileCnt++;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
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
            log4j.debug("add mimeType=" + mimeType);
            return mimeType;

        }  catch (Exception ex) {
            log4j.debug("WARNING tika exception:" + ex);
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
           log4j.debug("@@@DUMP(" + versionID + "):" + key);
           
       }
    }
    
    protected void dumpMap(String header, File manifestFile, boolean doPrint, boolean doLog4j)
        throws TException
    {
        String oldMapS = FileUtil.file2String(oldManFile);
        String msg = "******* " + header + " >>>>>>>>\n"
                + oldMapS + "\n"
                + "<<<<<<< " + header + " ********\n";
        if (doPrint) {
            log4j.debug(msg);
        }
        if (doLog4j) {
            log4j.debug(msg);
        }
    }

    protected void logCounts()
    {
        try {
            DateState current = new DateState();
            String completion = current.getIsoDate();
            DateState lastAddVersion = versionMap.getLastAddVersion();
            DateState startDate = changeTokenCC.getStartDate();
            String startDateS = "not found";
            if (startDate != null) {
                startDateS = startDate.getIsoDate();
            }
            String currentDate = lastAddVersion.getIsoDate();
            log4j.debug(jsonCounts.toString(2));
            JSONObject jsonStatus = new JSONObject();
            //LinkedHashMap<String, Object> counts = new LinkedHashMap<>();
            //JSONObject jsonCounts = new JSONObject();
            jsonStatus.put("inComponent", inComponentCnt);
            jsonStatus.put("outComponent", outComponentCnt);
            jsonStatus.put("noChange", noChangeCnt);
            jsonStatus.put("change", changeCnt);
            jsonStatus.put("saveFileCnt", saveFileCnt);
            jsonStatus.put("totalSaveBytes", totalSaveBytes);
            jsonStatus.put("duplicateDeleteCnt", duplicateDeleteCnt);
            jsonStatus.put("addComponents", addComponents.size());
            jsonStatus.put("deleteComponents", deleteComponents.size());
            jsonStatus.put("totalDeleteBytes", totalDeleteBytes);
            jsonStatus.put("totalDiffBytes", (totalSaveBytes-totalDeleteBytes));
            jsonStatus.put("duplicateKeys", duplicateCnt);
            jsonStatus.put("matchKey", matchKeyCnt);
            jsonStatus.put("mimeChange", mimeChangeCnt);
            jsonStatus.put("mimeMatch", mimeMatchCnt);
            log4j.debug(">>>jsonStatus\n" + jsonStatus.toString(2));
            
            JSONObject jsonRun = new JSONObject();
            jsonRun.put("newManifestWritten", newManifestWritten);
            jsonRun.put("runAdd", runAdd);
            jsonRun.put("runProvenance", runProvenance);
            jsonRun.put("runDelete", runDelete);
            
            JSONObject tallyJSON = tallyComponent2JSON();
            jsonCounts.put("tallyVersion", tallyJSON);
            jsonCounts.put("processCounts", jsonStatus);
            jsonCounts.put("runCounts", jsonRun);
            
            jsonCounts.put("run", runS3);
            jsonCounts.put("status", "ok");
            jsonCounts.put("objectID", objectID.getValue());
            jsonCounts.put("node", "" + nodeID);
            jsonCounts.put("currentDate", currentDate);
            jsonCounts.put("fixDate", completion);
            jsonCounts.put("startDate", startDateS);
            jsonCounts.put("collection", collection);
            log4j.debug(">>>jsonCounts\n" + jsonCounts.toString(2));
            jsonResponse = new JSONObject();
            jsonResponse.put("changeToken", jsonCounts);
            //LogManager.getLogger().info(counts);
            addEntry("info", jsonResponse);
            //log4j.info(jsonCounts);
            //logFix.info(jsonResponse.toString(2));
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void addEntry(String levelS, JSONObject jsonEntry)
        throws TException
    {
        try {
            Level level = Level.toLevel(levelS, Level.INFO);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonEntry.toString());
            logJson.log(level, jsonNode);
            
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION(ex);
        }
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
        jsonCounts = changeTokenCC.getOutVerifyJson();
    }
    

    public JSONObject getJsonResponse() {
        return jsonResponse;
    }
}
