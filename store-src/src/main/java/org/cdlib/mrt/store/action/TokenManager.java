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
package org.cdlib.mrt.store.action;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.cdlib.mrt.store.TokenStatus;
import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.cloud.ManifestSAX;

import org.cdlib.mrt.store.action.ArchiveComponent;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.action.CloudActionAbs;
import static org.cdlib.mrt.cloud.action.CloudActionAbs.makeGeneralTException;
import org.cdlib.mrt.cloud.utility.NormVersionMap;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowAdd;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.store.KeyFileInf;
import static org.cdlib.mrt.store.action.ArchiveComponentList.MESSAGE;
import static org.cdlib.mrt.store.can.CAN.getArchiveName;
import org.cdlib.mrt.utility.ArchiveBuilder;
import org.cdlib.mrt.utility.ArchiveBuilderBase;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.PairtreeUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * This object imports the formatTypes.xml and builds a local table of supported format types.
 * Note, that the ObjectFormat is being deprecated and replaced by a single format id (fmtid).
 * This change is happening because formatName is strictly a description and has no functional
 * use. The scienceMetadata flag is being dropped because the ORE Resource Map is more flexible
 * and allows for a broader set of data type.
 * 
 * @author dloy
 */
public class TokenManager
{
    
    
    private static final String NAME = "TokenManager";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    public enum PostContent {full, producer} 
    
    protected TokenStatus token = null;
    protected NodeIO.AccessNode extractAccessNode = null;
    protected NodeIO.AccessNode deliveryAccessNode = null;
    protected PostContent postContent = null;
    
    protected VersionMap versionMap = null;
    protected NodeIO nodeIO = null;
    protected LoggerInf logger = null;
    
    
    public static void main(String[] args) 
        throws TException
    {
        TokenStatus tokenStatus = null;
        String key = null;
        try {
            String nodeIOName = "nodes-stage";
            long extractNode = 9502L;
            long deliveryNode = 7001L;
            Identifier objectID = new Identifier("ark:/99999/fk4806c36f");
            int versionID = 0;
            String archiveTypeS = "targz";
            String content = "producer";
            Boolean fullObject = null;
            Boolean returnOnError = true;
            String [] shortlist  = {
                "mrt-erc.txt",
                "mrt-oaidc.xml",
                "stash-wrapper.xml"
            };
            ArrayList<String> producerFilter = new ArrayList<>();
            for (String name : shortlist) {
                producerFilter.add(name);
            }   
            LoggerInf logger = LoggerAbs.getTFileLogger("testFormatter", 10, 10);
            
            TokenManager tokenManager = TokenManager.getNewTokenManager(
                nodeIOName,
                extractNode,
                deliveryNode,
                objectID,
                versionID,
                archiveTypeS,
                fullObject,
                returnOnError,
                content,
                producerFilter,
                logger);
            
            tokenStatus = tokenManager.getTokenStatus();
            String tokenID = tokenStatus.getToken();
            key = tokenID + "/status";
            String jsonOut=tokenStatus.getJson();
            System.out.println("JSONOUT:" + jsonOut);
            System.out.println("completion date:" + tokenStatus.getAnticipatedAvailableDate().getIsoDate());
            TokenManager.saveCloudToken(nodeIOName, deliveryNode, tokenStatus, logger);
            if (tokenStatus.getExMsg() != null) {
                System.out.println("saveCloudToken ExMsg:" + tokenStatus.getExMsg());
                return;
            }
            TokenStatus tokenStatus2 = TokenManager.getCloudToken(nodeIOName, deliveryNode, tokenID, logger);
            if (tokenStatus2.getExMsg() != null) {
                System.out.println("getCloudToken ExMsg:" + tokenStatus2.getExMsg());
                return;
            }
            
            String jsonOut2=tokenStatus2.getJson();
            System.out.println("JSONOUT2:" + jsonOut2);
            
            if (false) return;
            CloudResponse response = TokenManager.deleteCloudToken(nodeIOName, deliveryNode, tokenID, logger);
            System.out.println("Delete:" + response.getHttpStatus());

        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();
            
        } finally {
        }
        
    }
    

    public static TokenManager getNewTokenManager(
            String nodeIOName,
            Long extractNode,
            Long deliveryNode,
            Identifier objectID,
            Integer versionID,
            String archiveTypeS,
            Boolean fullObject,
            Boolean returnOnError,
            String content,
            List filterList,
            LoggerInf logger)
        throws TException
    {
        return new TokenManager(
            nodeIOName,
            extractNode,
            deliveryNode,
            objectID,
            versionID,
            archiveTypeS,
            fullObject,
            returnOnError,
            content,
            filterList,
            logger);
    }
    
    private TokenManager(
            String nodeIOName,
            Long extractNode,
            Long deliveryNode,
            Identifier objectID,
            Integer versionID,
            String archiveTypeS,
            Boolean fullObject,
            Boolean returnOnError,
            String content,
            List filterList,
            LoggerInf logger)
        throws TException
    {
        if (content == null) content = "full";
        this.postContent = PostContent.valueOf(content);
        this.token = TokenStatus.getTokenStatus();
        this.token.setNodeIOName(nodeIOName);
        this.token.setExtractNode(extractNode);
        this.token.setDeliveryNode(deliveryNode);
        this.token.setObjectID(objectID);
        this.token.setVersionID(versionID);
        this.token.setArchiveType(archiveTypeS);
        this.token.setFullObject(fullObject);
        this.token.setReturnOnError(returnOnError);
        this.token.setFilterList(filterList);
        if (content == null) {
            this.postContent = PostContent.full;
        }
        this.logger = logger;
        validate();
        this.nodeIO = NodeIO.getNodeIO(nodeIOName, logger);
        getAccessNodes();
        setToken();
        setArchiveContent();
        setSize();
        if (token.getArchiveContent() == TokenStatus.ArchiveContent.producer) {
            if (token.getFilterList() == null) {
                token.setFilterList(setProducerFilterDefault());
            }
        }
        setAnticipatedDate();
    }
    
    public TokenManager getExistingTokenManager(
            String nodeIOName,
            Long deliveryNode,
            String token,
            LoggerInf logger)
        throws TException
    {
        return new TokenManager(
            nodeIOName,
            deliveryNode,
            token,
            logger);
    }
    
    private TokenManager(
            String nodeIOName,
            Long deliveryNode,
            String token,
            LoggerInf logger)
        throws TException
    {
        try {
            if (nodeIOName == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIOName required and missing");
            }
            if (deliveryNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            if (token == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            deliveryAccessNode = NodeIO.getCloudNode(nodeIOName, deliveryNode, logger);
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    private void validate() 
        throws TException
    {
        if (token.getNodeIOName() == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIOName required and missing");
        }
        if (token.getExtractNode() == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "extractNode required and missing");
        }
        if (token.getDeliveryNode() == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
        }
        if (token.getObjectID() == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID required and missing");
        }
        if (token.getVersionID() != null) {
            if (token.getVersionID() < 0) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "versionID invalid:" + token.getVersionID());
            }
        }
        if (token.getArchiveType() == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "archiveType required and missing");
        }
    }
    
    private void getAccessNodes() 
        throws TException
    {
        try {
            extractAccessNode = nodeIO.getAccessNode(token.getExtractNode());
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "extractNode not found:"
                + " - nodeIOName:" + token.getNodeIOName()
                + " - extractNode:" + token.getExtractNode()
                    
            );
        }
        try {
            deliveryAccessNode = nodeIO.getAccessNode(token.getDeliveryNode());
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryAccessNode not found:"
                + " - nodeIOName:" + token.getNodeIOName()
                + " - extractNode:" + token.getExtractNode()
                    
            );
        }
    }
    
    private void getDeliveryAccessNodes() 
        throws TException
    {
        try {
            extractAccessNode = nodeIO.getAccessNode(token.getExtractNode());
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "extractNode not found:"
                + " - nodeIOName:" + token.getNodeIOName()
                + " - extractNode:" + token.getExtractNode()
                    
            );
        }
        try {
            deliveryAccessNode = nodeIO.getAccessNode(token.getDeliveryNode());
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryAccessNode not found:"
                + " - nodeIOName:" + token.getNodeIOName()
                + " - extractNode:" + token.getExtractNode()
                    
            );
        }
    }
    
    private void setArchiveContent()
        throws TException
    {
        try {
            if (postContent == PostContent.producer) {
                token.setArchiveContent(TokenStatus.ArchiveContent.producer);
                if (token.getVersionID() == null) {
                    token.setVersionID(0);
                }
            } else if (token.getVersionID() == null) {
                token.setArchiveContent(TokenStatus.ArchiveContent.object);

            } else {
                token.setArchiveContent(TokenStatus.ArchiveContent.version);
            }
            setVersionMap();
            if ((token.getVersionID() != null) && (token.getVersionID() == 0)) {
                token.setVersionID(versionMap.getCurrent());
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
        
    }
    
    private void setVersionMap()
            throws TException
    {
        try {
            Identifier objectID = token.getObjectID();
            CloudStoreInf s3service = extractAccessNode.service;
            String bucket = extractAccessNode.container;
            InputStream manifestXMLIn = s3service.getManifest(bucket, objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            this.versionMap = ManifestSAX.buildMap(manifestXMLIn, logger);
            if (DEBUG) System.out.println("versionMap dump:\n"
                    + " - getActualSize:" + versionMap.getActualSize() + "\n"
                    + " - getTotalSize:" + versionMap.getTotalSize() + "\n"
                    + " - getOriginalActualSize:" + versionMap.getOriginalActualSize() + "\n"
                    + " - getOriginalTotalSize:" + versionMap.getOriginalTotalSize() + "\n"
                    + " - getUniqueSize:" + versionMap.getUniqueSize() + "\n"
            );

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    protected void setToken()
            throws TException
    {
        UUID uuid = UUID.randomUUID();
        token.setToken(uuid.toString());
    }
    
    private void setSize()
            throws TException
    {
        try {
            switch (token.getArchiveContent()) {
                case object:
                    token.setCloudContentBytes(getObjectSize());
                    break;
                case version:
                    token.setCloudContentBytes(getVersionSize());
                    break;
                case producer:
                    token.setCloudContentBytes(getProducerSize());
                    break;
            }
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    protected long getObjectSize()
        throws TException
    {
        if (token.getFullObject() == null) {
            token.setFullObject(false);
        }
        
        if (token.getFullObject()) {
            return versionMap.getTotalSize();
        }
        return versionMap.getActualSize();
    }
    
    protected long getVersionSize()
        throws TException
    {
        VersionMap.VersionStats stats = versionMap.getVersionStats(token.getVersionID());
        return stats.totalSize;
    }
    
    protected long getProducerSize()
        throws TException
    {
        List<FileComponent> components = versionMap.getVersionComponents(token.getVersionID());
        long size = 0;
        for (FileComponent component : components) {
            String identifier = component.getIdentifier();
            if (identifier.startsWith("system/")) continue;
            for (String filter : token.getFilterList()) {
                if (identifier.endsWith(filter)) continue;
            }
            size += component.getSize();
        }
        return size;
    }
    
    private void setAnticipatedDate()
        throws TException
    {
        long bytes = token.getCloudContentBytes();
        
        double bytesD = bytes;
        double secondsD = 20 + ((bytesD/1000000000) * 60);
        long seconds = (long)secondsD;
        System.out.println("bytes:" + bytes + " - seconds:" + seconds);
        setAnticipatedDate(seconds);
    }
    
    public void setAnticipatedDate(long seconds)
        throws TException
    {
        token.buildApproximateCompletionDate(seconds);
    }
    
    
    
    public String saveCloudToken()
        throws TException
    {
        return saveCloudToken(token.getNodeIOName(), token.getDeliveryNode(), getTokenStatus(), logger);
    }
    
    public static String saveCloudToken(
            String nodeIOName,
            Long deliveryNode,
            TokenStatus tokenStatus,
            LoggerInf logger)
        throws TException
    {
        File createFile = null;
        String tokenStatusJSON = "{ }";
        try {
            if (nodeIOName == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIOName required and missing");
            }
            if (deliveryNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            if (tokenStatus == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            NodeIO.AccessNode deliveryAccessNode = NodeIO.getCloudNode(nodeIOName, deliveryNode, logger);
            CloudStoreInf cloudService = deliveryAccessNode.service;
            String bucket = deliveryAccessNode.container;
            String key = tokenStatus.getToken() + "/status";
            
            try {
                cloudService.deleteObject(bucket, key);
            } catch (Exception ex) { }
            
            tokenStatusJSON = tokenStatus.getJson();
            createFile = FileUtil.getTempFile("token.", ".txt");
            FileUtil.string2File(createFile, tokenStatusJSON);
            CloudResponse response = cloudService.putObject(bucket, key, createFile);
            Exception ex = response.getException();
            int putStatus = response.getHttpStatus();
            if (ex != null) {
                String exMsg = MESSAGE + "saveToken Exception:" 
                        + " - key=" + key
                        + " - status=" + putStatus
                        + " - exception:" + ex
                        ;
                logger.logError(MESSAGE + exMsg, 1);
                tokenStatus.setExStatus(putStatus);
                tokenStatus.setExMsg(exMsg);
                
                if (ex instanceof TException) {
                    throw (TException) ex;
                } else {
                    throw new TException(ex);
                }
            }
            
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
            
        } finally {
            try {
                createFile.delete();
            } catch (Exception ex) { }
            return tokenStatusJSON;
        }
    }
    
    public static TokenStatus getCloudToken(
            String nodeIOName,
            Long deliveryNode,
            String token,
            LoggerInf logger)
        throws TException
    {
        
        File createFile = null;
        try {
            if (nodeIOName == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIOName required and missing");
            }
            if (deliveryNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            if (token == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            NodeIO.AccessNode deliveryAccessNode = NodeIO.getCloudNode(nodeIOName, deliveryNode, logger);
            CloudStoreInf cloudService = deliveryAccessNode.service;
            String bucket = deliveryAccessNode.container;
            
            createFile = FileUtil.getTempFile("token.", ".txt");
            String key = token + "/status";
            CloudResponse response = new CloudResponse(bucket, key);
            cloudService.getObject(bucket, key, createFile, response);
            Exception ex = response.getException();
            if (ex != null) {
                ex.printStackTrace();
                throw ex;
            }
            String tokenStatusJ = FileUtil.file2String(createFile);
            TokenStatus tokenStatus = TokenStatus.getTokenStatusFromJson(tokenStatusJ);
            return tokenStatus;
            
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
            
        } finally {
            try {
                createFile.delete();
            } catch (Exception ex) { }
        }
    }
    
    public static void getCloudData(
            String nodeIOName,
            Long deliveryNode,
            String token,
            File createFile,
            LoggerInf logger)
        throws TException
    {
        
        try {
            if (nodeIOName == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIOName required and missing");
            }
            if (deliveryNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            if (token == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            NodeIO.AccessNode deliveryAccessNode = NodeIO.getCloudNode(nodeIOName, deliveryNode, logger);
            CloudStoreInf cloudService = deliveryAccessNode.service;
            String bucket = deliveryAccessNode.container;
            
            String key = token + "/data";
            
            CloudResponse response = new CloudResponse(bucket, key);
            cloudService.getObject(bucket, key, createFile, response);
            Exception ex = response.getException();
            if (ex != null) {
                ex.printStackTrace();
                throw ex;
            }
            
            
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
            
        } finally {
        }
    }
    
    public static CloudResponse deleteCloudToken(
            String nodeIOName,
            Long deliveryNode,
            String token,
            LoggerInf logger)
        throws TException
    {
        
        try {
            if (nodeIOName == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIOName required and missing");
            }
            if (deliveryNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            if (token == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            NodeIO.AccessNode deliveryAccessNode = NodeIO.getCloudNode(nodeIOName, deliveryNode, logger);
            CloudStoreInf cloudService = deliveryAccessNode.service;
            String bucket = deliveryAccessNode.container;
            
            String key = token + "/status";
            return cloudService.deleteObject(bucket, key);
            
        } catch (Exception ex) { 
            System.out.println("Delete ex:" + ex);
            return null;
        }
    }
    
    public static CloudResponse deleteCloudData(
            String nodeIOName,
            Long deliveryNode,
            String token,
            LoggerInf logger)
        throws TException
    {
        
        try {
            if (nodeIOName == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIOName required and missing");
            }
            if (deliveryNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            if (token == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryNode required and missing");
            }
            NodeIO.AccessNode deliveryAccessNode = NodeIO.getCloudNode(nodeIOName, deliveryNode, logger);
            CloudStoreInf cloudService = deliveryAccessNode.service;
            String bucket = deliveryAccessNode.container;
            
            String key = token + "/data";
            return cloudService.deleteObject(bucket, key);
            
        } catch (Exception ex) { 
            System.out.println("Delete ex:" + ex);
            return null;
        }
    }
    
    public static List<String> setProducerFilterDefault()
        throws TException
    {
        System.out.println("setProducerFilterDefault");
        String [] shortlist  = {
                "mrt-erc.txt",
                "mrt-eml.txt",
                "mrt-dc.xml",
                "mrt-delete.txt",
                "mrt-dua.txt",
                "mrt-dataone-manifest.txt",
                "mrt-datacite.xml",
                "mrt-embargo.txt",
                "mrt-oaidc.xml",
                "stash-wrapper.xml"
            };
        ArrayList<String> producerFilter = new ArrayList<>();
        for (String name : shortlist) {
            producerFilter.add(name);
        }
        return producerFilter;
    }

    public TokenStatus getTokenStatus() {
        return token;
    }
}
