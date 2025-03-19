
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

import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.VersionMapUtil;
import org.cdlib.mrt.cloud.action.CloudActionAbs;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.store.KeyFileInf;
import static org.cdlib.mrt.store.fix.CloudFixAbs.makeGeneralTException;
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
public class ChangeTokenCC
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
    protected DateState startDate = null;
    
   
    
    protected int version = 0;
    protected CloudList fullCloudList = null;
    protected ArrayList<LinkedHashMap<String, ChangeComponent>> outComponents = new ArrayList<LinkedHashMap<String, ChangeComponent>>();
    //protected ArrayList<ChangeComponent> addVersionComponents = new ArrayList<>();
    //protected LinkedHashMap<String, FileComponent> deleteComponents = new LinkedHashMap<String, FileComponent>();
    //protected VersionMap outVersionMap = null;
    //protected long totalDeleteBytes = 0;
    //protected long totalSaveBytes = 0;
    protected int versionDuplicateCnt = 0;
    protected int deleteCnt = 0;
    //protected int duplicateDeleteCnt = 0;
    protected int changeCnt = 0;
    protected int noChangeCnt = 0;
    protected int duplicateCnt = 0;
    protected int inComponentCnt = 0;
    protected int outComponentCnt = 0;
    protected int matchKeyCnt = 0;
    protected int previousFixCnt = 0;
    protected JSONObject outVerifyJson = new JSONObject();
    //protected int newKeyCnt = 0;
    //protected int saveFileCnt = 0;
    //protected int mimeChangeCnt = 0;
    protected int mimeMatchCnt = 0;
    HashMap<Integer,Integer> inVersionCnt = new HashMap<>();
    HashMap<Integer,Integer> outVersionCnt = new HashMap<>();
    
                    
    public ChangeTokenCC(
            NodeIO nodeIO, 
            Long nodeID, 
            Identifier objectID, 
            LoggerInf logger)
        throws TException
    {
        super(nodeIO, nodeID, objectID, logger);;
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
                System.out.println("version:" + verid
                    + " - verin=" + inVersionCnt.get(verid)
                    + " - verout=" + outVersionCnt.get(verid)
                );
                outComponentCnt += outVersionCnt.get(verid);
            }
            
            dumpCounts();
            JSONArray stats = outVerify(objectCurrent, outComponents);
            outVerifyJson.put("objectID", objectID.getValue());
            outVerifyJson.put("versionCnt", objectCurrent);
            outVerifyJson.put("versions", stats);
            log4j.debug("validateObject" + outVerifyJson.toString(2));
            logFix.info(outVerifyJson); 
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
        LinkedHashMap<String, ChangeComponent>  newVersion = new LinkedHashMap<String, ChangeComponent>();
        try {
            List<FileComponent> currentVersion = getVersion(versionID);
            inVersionCnt.put(versionID, currentVersion.size());
            for (FileComponent currentComponent : currentVersion) {
                processComponent(versionID, currentComponent, newVersion);
            }
            outVersionCnt.put(versionID, newVersion.size());
            outComponents.add(newVersion);
            int failCnt = verifyVersion(currentVersion, newVersion);
            if (failCnt != 0) {
                throw new TException.INVALID_ARCHITECTURE("Bad output version:" + failCnt);
            } else {
                log4j.debug("Version verified:" + versionID);
            }
            if (versionID == objectCurrent) {
                ChangeComponent pCC = getProvenanceCC(objectID, versionID);
                String fileID = pCC.getOutComponent().getIdentifier();
                newVersion.put(pCC.getOutComponent().getIdentifier(), pCC);
                pCC.dump("old man");
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
            LinkedHashMap<String, ChangeComponent>  newVersionHash)
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
    
    
    
    protected LinkedHashMap<String, ArrayList<FileComponent>> getHashVersionDigests(
            LinkedHashMap<String, ChangeComponent>  hashFileID)
    {
        LinkedHashMap<String, ArrayList<FileComponent>> hashVersionDigests = new LinkedHashMap<>();
        Set<String> fileIDs = hashFileID.keySet();
        for (String fileID: fileIDs) {
            ChangeComponent changeComponent = hashFileID.get(fileID);
            FileComponent component = changeComponent.getOutComponent();
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
            LinkedHashMap<String, ChangeComponent>  newVersion)
        throws TException
    { 
        String fileID = currentComponent.getIdentifier();
        String originalID = fileID;
        inComponentCnt++;
        if (startDate == null) {
            startDate = currentComponent.getCreated();
        }
        try {
          
            if (!fileID.contains(TEST_TOKEN)) {
                ChangeComponent cc = getCCAsis(versionID, currentComponent, currentComponent);
                newVersion.put(fileID, cc);
                noChangeCnt++;
                log4j.trace(">>>no TEST_TOKEN(" + versionID + ")=" + fileID);
                return;
            }
            
            // if fixed fileID results is unchanged then no further action required
            String extractFileID = fixToken(fileID);
            if (extractFileID.equals(fileID)) {
                ChangeComponent cc = getCCAsis(versionID, currentComponent, currentComponent);
                noChangeCnt++;
                log4j.debug(">>>equalID(" + versionID + ")=" + fileID);
                return;
            }
            
            // ERROR situation where a fixed fileID already exists in version
            // fileID must be unique to version
            // no processing on corrected fileID - duplicate dropped
            fileID = extractFileID;
            ChangeComponent checkComponent = newVersion.get(fileID);
            if (checkComponent != null) {
                versionDuplicateCnt++;
                log4j.warn("Duplicate detected:"
                        + " - objectID:" + objectID.getValue()
                        + " - versionID:" + versionID
                        + " - fileID:" + fileID
                );
                duplicateCnt++;
                log4j.info(">>>skip version contains multiple(" + versionID + ")=" + fileID);
                return;
            }
            log4j.trace(">>>orgID(" + versionID + ")=" + originalID + "\n"
                    + ">>>modID(" + versionID + ")=" + fileID 
            );
            
            // Test new key (+content) needs to be added
            ChangeComponent cc = setNewComponent(versionID, fileID, currentComponent);
            FileComponent addComponent = cc.getOutComponent();
            int addVersion = addDigestFileid(versionID, addComponent);
            log4j.trace(">>>mcKey(" + versionID + "," + addVersion+ ")=" + addComponent.getLocalID()+ "\n");
            newVersion.put(fileID, cc);
            changeCnt++;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    protected ChangeComponent getCCAsis(
            int versionID,
            FileComponent inComponent,
            FileComponent outComponent)
        throws TException
    {
        ChangeComponent component = ChangeComponent.get(nodeID, objectID, versionID, inComponent);
        Integer digestVersion = getDigestFileidVersion(inComponent);
        if (digestVersion != null) {
            if (digestVersion < versionID) {
                if (inComponent.getLocalID().contains("|" + versionID + "|")) {
                    component.setOp(ChangeComponent.Operation.previous_fix);
                    FileComponent fixComponent = new FileComponent();
                    fixComponent.copy(inComponent);
                    String fixKey = inComponent.getLocalID().replace("|" + versionID + "|", "|" + digestVersion + "|");
                    System.out.println("***GETCCASIS FIX***"
                            + "\n - localid:" + inComponent.getLocalID()
                            + "\n - fixKey: " + fixKey
                    );
                    fixComponent.setLocalID(fixKey);
                    component.setOutComponent(fixComponent);
                    previousFixCnt++;
                    return component;
                }
                
            }
        }
        component.setOutComponent(outComponent);
        if (inComponent.getLocalID().contains("|" + versionID + "|"))
            component.setOp(ChangeComponent.Operation.asis_data);
        else component.setOp(ChangeComponent.Operation.asis_reference);
        
        addDigestFileid(versionID, inComponent);
        log4j.trace(">>>getCCAsis - " 
        //System.out.println(">>>getCCAsis - " 
                + "versionID:" + versionID
                + " - inComponent.getLocalID():" + inComponent.getLocalID()
                + " - component.setOp:" + component.getOp().toString()
        );
        return component;
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
                ChangeComponent extChangeComponent = componentHash.get(key);
                FileComponent extComponent = extChangeComponent.getOutComponent();
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
            log4j.debug("!!!getNewVersionList(" + versionID + ") tokenCnt=" + tokenCnt);
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
    
    protected ChangeComponent setNewComponent(int versionID, String fileID, FileComponent currentComponent)
        throws TException
    {
        try {
            ChangeComponent retComponent = ChangeComponent.get(nodeID, objectID, versionID, currentComponent);
            FileComponent newComponent = new FileComponent();
            retComponent.setOutComponent(newComponent);
            newComponent.copy(currentComponent);
            newComponent.setIdentifier(fileID);
            String newLocalID = objectID.getValue() + "|" + versionID + "|" + fileID;
            if (versionID == 1) {
                newComponent.setLocalID(newLocalID);
                retComponent.setOp("move_data");
                return retComponent;
            }
            
            // match this component against previous versions for setting local key pointer
            for (LinkedHashMap<String, ChangeComponent> verComponents : outComponents) {
                ChangeComponent ccComponent = verComponents.get(fileID);
                if (ccComponent == null) continue;
                
                FileComponent matchComponent = ccComponent.outComponent;
                
                // no match this version
                // see if real match by comparing length/digest
                if (versionMap.isMatch(matchComponent, newComponent)) {
                    newComponent.setLocalID(matchComponent.getLocalID());
                    matchKeyCnt++;
                    
                    if (false) logFix.info("matchFileComponent:" 
                            + " - ark=" + objectID.getValue()
                            + " - versionID=" + versionID
                            + "\n"
                            + " - matchKey:" + matchComponent.getLocalID() + "\n"
                    );
                    retComponent.setOp("move_reference");
                    return retComponent;
                }
            }
            
            // no earlier version matches this fileID - do addFile processing
            
            newComponent.setLocalID(newLocalID);
            retComponent.setOp("move_data");
            return retComponent;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected ChangeComponent getProvenanceCC(Identifier objectID, int currVersion)
            throws TException
    {
        try {
            String manifestKey = objectID.getValue() + "|manifest";
            if (oldManResponse == null) {
                throw new TException.INVALID_OR_MISSING_PARM("oldManifestResponse required for processing");
            }
            FileComponent oldFileComponent = new FileComponent();
            oldFileComponent.setPrimaryID("manifest.xml");
            oldFileComponent.setLocalID(manifestKey);
            oldFileComponent.addMessageDigest(oldManResponse.getSha256(), "sha256");
            oldFileComponent.setSize(oldManResponse.getStorageSize());
            oldFileComponent.setMimeType("application/xml");
            
            //dumpManFile("getProvenanceCC", oldManFile);
            String newFileID = "system/provenance_manifest.xml";
            ChangeComponent provenanceChangeComponent = setNewComponent(currVersion, newFileID, oldFileComponent);
            provenanceChangeComponent.setOp(ChangeComponent.Operation.provenance);
            return provenanceChangeComponent;

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
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
            count.put("duplicateKeys", duplicateCnt);
            count.put("matchKey", matchKeyCnt);
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
            jsonCounts.put("duplicateKeys", duplicateCnt);
            jsonCounts.put("matchKey", matchKeyCnt);
            jsonCounts.put("mimeMatch", mimeMatchCnt);
            jsonCounts.put("previousFix", previousFixCnt);
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
    
    public JSONArray outVerify(
            int inVersions, 
            ArrayList<LinkedHashMap<String, ChangeComponent>> outComponents)
        throws TException
    {
        if (inVersions != outComponents.size()) {
            throw new TException.INVALID_CONFIGURATION("version counts do not match:" 
                    + " - inVersions=" + inVersions
                    + " - outComponents.size=" + outComponents.size()
            );
        }
        //JSONArray validateObject = new JSONArray();
        JSONArray validateArray = new JSONArray();
        int outVersionNum = 0;
        for (LinkedHashMap<String, ChangeComponent> outVersion : outComponents) {
            outVersionNum++;
            JSONObject validateVersion = validateOutVersion(outVersionNum, outVersion);
            validateVersion.put("versionNum", "" + outVersionNum);
            //validateObject.put("" + outVersionNum, validateVersion);
            validateArray.put(validateVersion); 
        }
        return validateArray;
    }
    
    public static JSONObject validateOutVersion(int outVersionNum, LinkedHashMap<String, ChangeComponent> outVersion)
        throws TException
    {
        LinkedHashMap<ChangeComponent.Operation, Integer> opCnts = new LinkedHashMap<>();
        int testCnt = 0;
        int sysCnt = 0;
        JSONObject validateVersion = new JSONObject();
        try {
            Set<String> keys = outVersion.keySet();
            for (String key : keys) {
                testCnt++;
                if (key.startsWith("system")) sysCnt++;
                ChangeComponent changeComponent = outVersion.get(key);
                ChangeComponent.Operation op = changeComponent.getOp();
                Integer opCnt = opCnts.get(op);
                if (opCnt == null) {
                    opCnt = 1;
                } else {
                    opCnt++;
                }
                opCnts.put(op, opCnt);
                FileComponent inComponent = changeComponent.getInComponent();
                FileComponent outComponent = changeComponent.getOutComponent();
                String inFileID = inComponent.getIdentifier();
                String outFileID = outComponent.getIdentifier();
                if ((changeComponent.getOp() == ChangeComponent.Operation.asis_data) 
                    || (changeComponent.getOp() == ChangeComponent.Operation.asis_reference))
                {
                    if (inComponent != outComponent) {

                        throw new TException.INVALID_CONFIGURATION("asis no match"
                                + " - version:" + changeComponent.getVersionID()
                                + " - inFileID:" + inComponent.getIdentifier()
                        );
                    }
                    if (inFileID.contains(MATCH_TOKEN)) {

                        throw new TException.INVALID_CONFIGURATION("asis contains " + MATCH_TOKEN
                                + " - version:" + changeComponent.getVersionID()
                                + " - inFileID:" + inComponent.getIdentifier()
                        );
                    }
                    continue;
                }
                if ((changeComponent.getOp() != ChangeComponent.Operation.provenance) 
                    && (!inFileID.contains(outFileID))) {
                    throw new TException.INVALID_CONFIGURATION("contains fail"
                                + " - inFileID:" + inFileID
                                + " - outFileID:" + outFileID
                        );
                }

            }
            //validateVersion.put("versionID", outVersionNum);
            validateVersion.put("testCnt", testCnt);
            //validateVersion.put("sysCnt", sysCnt);
            log4j.debug("***validateOutVersion(" + outVersionNum + ") "
                    + " - testCnt=" + testCnt
                    + " - sysCnt=" + sysCnt
            );
            
            Set<ChangeComponent.Operation> opKeys = opCnts.keySet();
            for (ChangeComponent.Operation op : opKeys) {
                int value = opCnts.get(op);
                validateVersion.put(op.toString(), value);
                log4j.debug("++optype(" + op.toString() + ")=" + value);
            }
            return validateVersion;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }

    public ArrayList<LinkedHashMap<String, ChangeComponent>> getOutComponents() {
        return outComponents;
    }

    public int getDeleteCnt() {
        return deleteCnt;
    }

    public int getChangeCnt() {
        return changeCnt;
    }

    public int getNoChangeCnt() {
        return noChangeCnt;
    }

    public int getDuplicateCnt() {
        return duplicateCnt;
    }

    public int getInComponentCnt() {
        return inComponentCnt;
    }

    public int getOutComponentCnt() {
        return outComponentCnt;
    }

    public int getMatchKeyCnt() {
        return matchKeyCnt;
    }

    public JSONObject getOutVerifyJson() {
        return outVerifyJson;
    }

    public int getPreviousFixCnt() {
        return previousFixCnt;
    }

    public DateState getStartDate() {
        return startDate;
    }
    
    
}
