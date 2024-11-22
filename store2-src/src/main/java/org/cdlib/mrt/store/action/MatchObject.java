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
package org.cdlib.mrt.store.action;

import org.cdlib.mrt.store.fix.*;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;


import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
/**
 * Abstract for performing a fixity test
 * @author dloy
 */
public class MatchObject
{

    protected static final String NAME = "MatchObject";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String  STATUS_PROCESSING = "processing";
    private static final boolean DEBUG = false;
    
    public enum DiffOrigin { source, target};
    public enum DiffType {fileid, digest};
    public enum DiffStatus {match, digest_match, digest_match_count, digest_fail_count, digest_fail, digest_fail_id, fileid_fail, fileid_match, not_set};

    
    protected NodeIO nodeIO = null;
    protected NodeInfo source = null;
    protected NodeInfo target = null;
    protected ArrayList<DiffInfo> diffInfoList = new ArrayList<>();
    protected LoggerInf logger = null;
    protected int matchCnt = 0;
    protected int fileIDCnt = 0;
    protected int digestCnt = 0;
    protected int diffKeyMatchCnt= 0;
    protected int sameKeyMatchCnt = 0;
    protected HashMap<DiffStatus, Integer> diffCnts = new HashMap<>();
    //protected int digestCnt = 0;
    protected ChangeComponent oldMapCC = null;
    protected int objectCurrent = 0;
    protected static final Logger log4j = LogManager.getLogger();
    public MatchObject(NodeIO  nodeIO, Long nodeIDSource, Identifier objectIDSource, 
            Long nodeIDTarget, Identifier objectIDTarget, LoggerInf logger)
        throws TException
    {  
        this.logger = logger;
        if (nodeIO == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "nodeIO- required");
        }
        this.nodeIO = nodeIO;
        this.source = setNodeInfo(nodeIDSource, objectIDSource);
        this.target  = setNodeInfo(nodeIDTarget, objectIDTarget);
        matchInfo();
    }
    
    public MatchObject(VersionMap mapSource, Long nodeIDSource, Identifier objectIDSource, 
            VersionMap mapTarget, Long nodeIDTarget, Identifier objectIDTarget, LoggerInf logger)
        throws TException
    {  
        this.logger = logger;
        this.source = setNodeInfo(mapSource, nodeIDSource, objectIDSource);
        this.target  = setNodeInfo(mapTarget, nodeIDTarget, objectIDTarget);
        matchInfo();
    }
    
    private void matchInfo()
        throws TException
    {
        this.source.dumpMeta("source matchInfo");
        this.target.dumpMeta("target matchInfo");
    }
    
    private NodeInfo setNodeInfo(VersionMap versionMap, Long nodeID, Identifier objectID)
        throws TException
    {
         NodeInfo nodeInfo = new NodeInfo(nodeID, objectID);
         nodeInfo.setVersionMap(versionMap);
         setService(nodeInfo);
         return nodeInfo;
    }
    
    private NodeInfo setNodeInfo(Long nodeID, Identifier objectID)
        throws TException
    {
         NodeInfo nodeInfo = new NodeInfo(nodeID, objectID);
         setService(nodeInfo);
         return nodeInfo;
    }
    
    private void setService(NodeInfo nodeInfo)
        throws TException
    {
        if (nodeInfo.nodeID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "nodeID required");
        }
        if (nodeInfo.objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "ark - required");
        }
        try {
            getVersionMap(nodeInfo);
            nodeInfo.objectCurrent = nodeInfo.versionMap.getCurrent();
            buildTables(nodeInfo);
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
        
    }
    
    private void getVersionMap(NodeInfo nodeInfo)
        throws TException
    {
        if (nodeInfo.versionMap != null) {
            return;
        }
        if (nodeInfo.nodeID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "nodeID required");
        }
        if (nodeInfo.objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "ark - required");
        }
        try {
            if (nodeIO == null) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        MESSAGE + "nodeIO required");
            }
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(nodeInfo.nodeID);
            if (accessNode == null) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        MESSAGE + "nodeID not found:" + nodeInfo.nodeID);
            }
            nodeInfo.s3service = accessNode.service;
            nodeInfo.bucket = accessNode.container;
            InputStream manifestXMLIn = nodeInfo.s3service.getManifest(nodeInfo.bucket, nodeInfo.objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + nodeInfo.objectID.getValue());
            }
            nodeInfo.versionMap =  ManifestSAX.buildMap(manifestXMLIn, logger);
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
        
    }
    
    public void matchSourceTarget()
        throws TException
    {
        System.out.println("begin MatchSourceTarget");
        matchTest(DiffOrigin.source, source, target);
        matchTest(DiffOrigin.target, target, source);
    }
    
    public void printStatus()
        throws TException
    {
       
        
        System.out.println("end MatchSourceTarget");
        System.out.println("matchCnt:" + matchCnt);
        System.out.println("fileIDCnt:" + fileIDCnt);
        System.out.println("digestCnt:" + digestCnt);
        System.out.println("diffKeyMatchCnt:" + diffKeyMatchCnt);
        System.out.println("sameKeyMatchCnt:" + sameKeyMatchCnt);
        System.out.println("diffInfoList:" + diffInfoList.size());
        Set<DiffStatus> keys = diffCnts.keySet();
        System.out.println("DiffStatus");
        for (DiffStatus key : keys) {
            Integer cnt = diffCnts.get(key);
            System.out.println(key + " - " + cnt);
        };
    }
    
    public void dumpDiffList(String status)
        throws TException
    {
        int i=0;
        DiffStatus printStatus = DiffStatus.valueOf(status);
        for (DiffInfo diffInfo: diffInfoList) {
            if (diffInfo.diffStatus != printStatus) continue;
            i++;
            String dmp = diffInfo.dump("" + i);
            System.out.println(dmp);
        }
    }
    
    
    public ArrayList<DiffInfo> getDiffList(String status)
        throws TException
    {
        DiffStatus matchStatus = DiffStatus.valueOf(status);
        ArrayList<DiffInfo> diffList = new ArrayList<>();
        for (DiffInfo diffInfo: diffInfoList) {
            if (diffInfo.diffStatus != matchStatus) continue;
            diffList.add(diffInfo);
        }
        return diffList;
    }
    
    public void matchDigest()
        throws TException
    {
        testDigest(DiffOrigin.source, source, target);
        testDigest(DiffOrigin.target, target, source);
    }
    
    public void matchTest(DiffOrigin diffOrigin, NodeInfo fromNode, NodeInfo toNode)
        throws TException
    {
        testDigest(diffOrigin, fromNode, toNode);
        testFileId(diffOrigin, fromNode, toNode);
    }
    
    protected void testDigest(DiffOrigin diffOrigin, NodeInfo fromNode, NodeInfo toNode)
        throws TException
    {
        ArrayList<LinkedHashMap<String,ArrayList<FileComponent>>> fromDigestList = fromNode.digestList;
        ArrayList<LinkedHashMap<String,ArrayList<FileComponent>>> toDigestList = toNode.digestList;
        DiffType diffType = DiffType.digest;
        for (int i=0; i < fromDigestList.size(); i++) {
            int current = i + 1;
            LinkedHashMap<String,ArrayList<FileComponent>> fromMap = fromDigestList.get(i);
            LinkedHashMap<String,ArrayList<FileComponent>> toMap = null;
            if (current > toDigestList.size()) {
                toMap = null;
            } else {
                toMap = toDigestList.get(i);
            }
            Set<String> fromKeys = fromMap.keySet();
            for (String key : fromKeys) {
                if ((toMap == null) || (toMap.get(key) == null)) {
                    ArrayList<FileComponent> listFileComponent = fromMap.get(key);
                    addDiffList(DiffStatus.digest_fail, diffOrigin, diffType, current, listFileComponent);
                    addDiffCnt(DiffStatus.digest_fail);
                    return;
                }
                
                ArrayList<FileComponent> fromFileComponents = fromMap.get(key);
                ArrayList<FileComponent> toFileComponents = toMap.get(key);
                if (fromFileComponents.size() == toFileComponents.size()) {
                    addDiffCnt(DiffStatus.digest_match_count, fromFileComponents.size());
                } else {
                    addDiffCnt(DiffStatus.digest_fail_count, fromFileComponents.size());
                }
                for (FileComponent fromFileComponent : fromFileComponents) {
                    boolean match = false;
                    for (FileComponent toFileComponent : fromFileComponents) {
                       if (fromFileComponent.getIdentifier().equals(toFileComponent.getIdentifier())) {
                           match = true;
                           matchCnt++;
                           digestCnt++;
                           addDiffCnt(DiffStatus.digest_match);
                           break;
                       }
                    }
                    if (!match) {
                        addDiffCnt(DiffStatus.digest_fail_id);
                    }
                }
            }
        }
    }
    
    protected void testFileId(DiffOrigin diffOrigin, NodeInfo fromNode, NodeInfo toNode)
        throws TException
    {
        ArrayList<LinkedHashMap<String,FileComponent>> fromFileIds = fromNode.fileIdCnt;
        ArrayList<LinkedHashMap<String,FileComponent>> toFileIds = toNode.fileIdCnt;
        DiffType diffType = DiffType.fileid;
        for (int i=0; i < fromFileIds.size(); i++) {
            int current = i + 1;
            LinkedHashMap<String,FileComponent> fromMap = fromFileIds.get(i);
            LinkedHashMap<String,FileComponent> toMap = null;
            if (current > toFileIds.size()) {
                toMap = null;
            } else {
                toMap = toFileIds.get(i);
            }
            Set<String> fromKeys = fromMap.keySet();
            for (String key : fromKeys) {
                FileComponent fromFileComponent = fromMap.get(key);
                matchCnt++;
                fileIDCnt++;
                FileComponent toFileComponent = toMap.get(key);
                if ((toMap == null) || (toFileComponent == null)) {
                    addDiffCnt(DiffStatus.fileid_fail);
                    setDiffEntry(DiffStatus.fileid_fail, diffOrigin, diffType, current, fromFileComponent);
                } else {
                    addDiffCnt(DiffStatus.fileid_match);
                    if (!fromFileComponent.getLocalID().equals(toFileComponent.getLocalID())) {
                        diffKeyMatchCnt++;
                    } else {
                        sameKeyMatchCnt++;
                    }
                }
            }
        }
    }
    
    protected void addDiffList(DiffStatus diffStatus, DiffOrigin diffOrigin, DiffType diffType, 
            int version, 
            List<FileComponent> listFileComponent)
    {
        for (FileComponent fileComponent : listFileComponent) {
            setDiffEntry(diffStatus, diffOrigin, diffType, version, fileComponent);
        }
    }
    
    protected void setDiffEntry(DiffStatus diffStatus, DiffOrigin diffOrigin, DiffType diffType, int version, FileComponent fileComponent)
    {
        DiffInfo diffInfo = new DiffInfo(diffStatus, diffOrigin, diffType, version, fileComponent);
        diffInfoList.add(diffInfo);
    }
    
    public void addDiffCnt(DiffStatus diffStatus)
    {
        addDiffCnt(diffStatus, 1);
    }
    
    public void addDiffCnt(DiffStatus diffStatus, int add)
    {
        Integer cnt = diffCnts.get(diffStatus);
        if (cnt == null) {
            cnt = add;
        } else {
            cnt += add;
        }
        diffCnts.put(diffStatus, cnt);
    }
    
    public int getDiffCnt(String diffStatusS)
    {
        DiffStatus diffStatus = DiffStatus.valueOf(diffStatusS);
        Integer cnt = diffCnts.get(diffStatus);
        if (cnt == null) {
            cnt = 0;
        }
        return cnt;
    }


    public NodeInfo getSource() {
        return source;
    }

    public NodeInfo getTarget() {
        return target;
    }
    
    protected void log(String msg, int lvl)
    {
        //System.out.println(msg);
        logger.logMessage(msg, lvl, true);
    }
    
    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    protected void buildTables(NodeInfo nodeInfo)
            throws TException
    {
        try {
            buildDigestList(nodeInfo);
            buildFileIdCnt(nodeInfo);
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    protected void buildDigestList(NodeInfo nodeInfo)
            throws TException
    {
        ArrayList<LinkedHashMap<String,ArrayList<FileComponent>>> digestList = new ArrayList<>();
        int current = nodeInfo.objectCurrent;
        
        for (int version=1; version<=current; version++) {
            LinkedHashMap<String,ArrayList<FileComponent>> versionDigestList = getVersionDigestList(version, nodeInfo);
            digestList.add(versionDigestList);
        }
        nodeInfo.digestList =  digestList;
    }
    
    protected LinkedHashMap<String,ArrayList<FileComponent>>  getVersionDigestList(int version, NodeInfo nodeInfo)
            throws TException
    {
        try {
            LinkedHashMap<String,ArrayList<FileComponent>> digestList = new LinkedHashMap<>();
            VersionMap versionMap = nodeInfo.versionMap;
            List<FileComponent> components = versionMap.getVersionComponents(version);
            for (FileComponent component : components) {
                MessageDigest digest = component.getMessageDigest("sha256");
                String digestValue = digest.getValue();
                ArrayList<FileComponent> list = digestList.get(digestValue);
                if (list == null) {
                    list = new ArrayList<>();
                }
                list.add(component);
                digestList.put(digestValue, list);
            }
            return digestList;
        
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    protected void buildFileIdCnt(NodeInfo nodeInfo)
            throws TException
    {
        ArrayList<LinkedHashMap<String,FileComponent>> fileIdCnt = new ArrayList<>();
        int current = nodeInfo.objectCurrent;
        
        for (int version=1; version<=current; version++) {
            LinkedHashMap<String,FileComponent> versionFileIdCnt = getVersionFileIdCnt(version, nodeInfo);
            fileIdCnt.add(versionFileIdCnt);
        }
        nodeInfo.fileIdCnt =  fileIdCnt;
    }
    
    protected LinkedHashMap<String,FileComponent>  getVersionFileIdCnt(int version, NodeInfo nodeInfo)
            throws TException
    {
        try {
            LinkedHashMap<String,FileComponent> digestCnt = new LinkedHashMap<>();
            VersionMap versionMap = nodeInfo.versionMap;
            List<FileComponent> components = versionMap.getVersionComponents(version);
            for (FileComponent component : components) {
                String digestValue = component.getIdentifier();
                digestCnt.put(digestValue, component);
            }
            return digestCnt;
        
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    public static TException makeGeneralTException(
            LoggerInf logger,
            String msg,
            Exception ex)
    {
        logger.logError(msg + ex,
                LoggerInf.LogLevel.UPDATE_EXCEPTION);
        logger.logError(msg + " - trace:"
                + StringUtil.stackTrace(ex),
                LoggerInf.LogLevel.DEBUG);
        return new TException.GENERAL_EXCEPTION(
                msg +  "Exception:" + ex);
    }
    
    public static class NodeInfo {
        protected Long nodeID = null;
        protected Identifier objectID = null;
        protected CloudStoreInf s3service = null;
        protected String bucket = null;;
        protected Integer objectCurrent = null;
        protected VersionMap versionMap = null;
        public ArrayList<LinkedHashMap<String,ArrayList<FileComponent>>> digestList = null;
        public ArrayList<LinkedHashMap<String,FileComponent>> fileIdCnt = null;
        protected int componentMatch = 0;
        
        public NodeInfo(Long nodeID, Identifier objectID)
        {
            this.nodeID = nodeID;
            this.objectID = objectID;
        }
    
        public LinkedHashMap<String,ArrayList<FileComponent>> getVersionDigest(int versionID)
            throws TException
        {
            int arrId = versionID - 1;
            if (arrId < 1) {
                throw new TException.INVALID_OR_MISSING_PARM("versionID invalid:" + versionID);
            }
            if (versionID > digestList.size()) {
                throw new TException.INVALID_OR_MISSING_PARM("versionID > saved versions"
                        + " - versionID:" +  versionID
                        + " - versionDigestArray.size:" +  digestList.size()
                );
            }
            return digestList.get(arrId);
        }
    
        public LinkedHashMap<String,FileComponent> getVersionFileIdCnt(int versionID)
            throws TException
        {
            int arrId = versionID - 1;
            if (arrId < 1) {
                throw new TException.INVALID_OR_MISSING_PARM("versionID invalid:" + versionID);
            }
            if (versionID > fileIdCnt.size()) {
                throw new TException.INVALID_OR_MISSING_PARM("versionID > saved versions"
                        + " - versionID:" +  versionID
                        + " - versionDigestArray.size:" +  fileIdCnt.size()
                );
            }
            return fileIdCnt.get(arrId);
        }
        
        public void dumpFileIdCnt(String hdr)
        {
            System.out.println("\n*** fileIdList:" + hdr);
            int ver = 0;
            for (LinkedHashMap<String,FileComponent> versionFileIDCnt : fileIdCnt) {
                ver++;
                Set<String> keys = versionFileIDCnt.keySet();
                for (String key : keys) {
                    FileComponent count = versionFileIDCnt.get(key);
                    System.out.println("val(" + ver + "," + key + ")= " + count.getLocalID());
                }
            }
            
        }
        
        public void dumpDigestList(String hdr)
        {
            System.out.println("\n*** digestList:" + hdr);
            int ver = 0;
            for (LinkedHashMap<String,ArrayList<FileComponent>> versionDigestList : digestList) {
                ver++;
                Set<String> keys = versionDigestList.keySet();
                for (String key : keys) {
                    ArrayList<FileComponent> components = versionDigestList.get(key);
                    int cCnt = 0;
                    for (FileComponent component : components) {
                        cCnt++;
                        System.out.println("val(" + ver + "," + key + "):" + cCnt + ":" + component.getIdentifier());
                    }
                }
            }
            
        }
        
        
        public void dump(String hdr)
        {
            System.out.println("*** " + hdr + "\n"
                    + " - nodeID:" + nodeID + "\n"
                    + " - objectID:" + objectID.getValue() + "\n"
                    + " - bucket:" + bucket + "\n"
                    + " - objectCurrent:" + objectCurrent + "\n"
            );
            dumpDigestList("DigestList");
            dumpFileIdCnt("versionFileIdArray");
        }
        
        public void dumpMeta(String hdr)
        {
            System.out.println("*** " + hdr + "\n"
                    + " - nodeID:" + nodeID + "\n"
                    + " - objectID:" + objectID.getValue() + "\n"
                    + " - bucket:" + bucket + "\n"
                    + " - objectCurrent:" + objectCurrent + "\n"
            );
        }
        
        public void setVersionMap(VersionMap versionMap) {
            this.versionMap = versionMap;
        }
        
    }
    
    public static class DiffInfo {
        public DiffOrigin diffOrigin = null;
        public DiffType diffType = null;
        public int version = 0;
        public FileComponent fileComponent = null;
        public DiffStatus diffStatus = null;
        public DiffInfo(DiffStatus diffStatus, DiffOrigin diffOrigin, DiffType diffType, int version, FileComponent fileComponent)
        {
            this.diffStatus = diffStatus;
            this.diffOrigin = diffOrigin;
            this.diffType = diffType;
            this.version = version;
            this.fileComponent = fileComponent;
        }
        
        public String dump(String msg)
        {
            return "**" + msg + "**"
                    + " - " + diffStatus.toString() + "|"
                    + " - " + diffOrigin.toString() + "|"
                    + diffType.toString() + "|"
                    + version + "|"
                    + fileComponent.getIdentifier() + "|"
                    + fileComponent.getLocalID() + "|";
        }
        
    }
}

