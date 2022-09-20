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

import java.io.File;
import java.util.ArrayList;
import java.util.List;


import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;


import org.cdlib.mrt.store.TokenStatus;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
/**
 * Async container for creating a container object
 * @author dloy
 */
public class AsyncCloudArchive
        extends ActionAbs
        implements Runnable
{

    private static final String NAME = "AsyncCloudArchive";
    private static final String MESSAGE = NAME + ": ";
    
    protected static final boolean DEBUG = false;
    
    protected TokenStatus tokenStatus = null;
    protected CloudArchive cloudArchive = null;
    protected File workBase = null;
    protected Exception ex = null;
    protected FileContent archiveContent = null;
    protected NodeIO.AccessNode extractAccessNode = null;
    protected NodeIO.AccessNode deliveryAccessNode = null;
    
    // JSONOUT2:{"nodeIOName":"nodes-stage","extractNode":9502,"deliveryNode":7001,"objectIDS":"ark:/99999/fk4806c36f","versionID":16,"token":"0eb93d89-8df4-48cb-b26d-2bb3915d2f1f","approximateCompletionSeconds":1587634079897,"cloudContentBytes":228268282,"archiveContent":"producer","archiveType":"targz","tokenStatusEnum":"NotReady","returnOnError":true,"filterList":["mrt-erc.txt","mrt-oaidc.xml","stash-wrapper.xml"]}
    public static void main(String[] args) 
        throws TException
    {
        TokenStatus tokenStatus = null;
        String key = null;
        
        String outFileName = "/apps/replic/tasks/storage/200413-presign-object/out/out.tar.gz";
        String nodeIOName = "nodes-stage";
        long extractNode = 9502L;
        long deliveryNode = 7001L;
        Identifier objectID = new Identifier("ark:/99999/fk4806c36f");
        Integer versionID = null;
        String archiveTypeS = "targz";
        String content = "full";
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
        String tokenID = null;
        LoggerInf logger = LoggerAbs.getTFileLogger("testFormatter", 10, 10);
        NodeIO nodeIO = NodeIO.getNodeIOConfig("ssm:", logger);
        try {
            TokenManager tokenManager = TokenManager.getNewTokenManager(
                nodeIO,
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
            tokenID = tokenStatus.getToken();
            
            String jsonOut=tokenStatus.getJson();
            System.out.println("JSONOUT:" + jsonOut);
            
            TokenManager.saveCloudToken(nodeIOName, deliveryNode, tokenStatus, logger);
            if (tokenStatus.getExMsg() != null) {
                System.out.println("saveCloudToken ExMsg:" + tokenStatus.getExMsg());
                return;
            }
            NodeIO.AccessNode deliveryAccessNode = nodeIO.getAccessNode(deliveryNode);
            TokenStatus tokenStatus2 = TokenManager.getCloudToken(nodeIO, deliveryNode, tokenID, logger);
            if (tokenStatus2.getExMsg() != null) {
                System.out.println("getCloudToken ExMsg:" + tokenStatus2.getExMsg());
                return;
            }
            
            String jsonOut2=tokenStatus2.getJson();
            System.out.println("JSONOUT2:" + jsonOut2);

        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();
            
        } finally {
        }
        
        try {
            AsyncCloudArchive aca = AsyncCloudArchive.getAsyncCloudArchive(
                tokenStatus,
                logger);
            aca.run();
            TokenStatus tokenStatus3 = TokenManager.getCloudToken(nodeIO, deliveryNode, tokenID, logger);
            if (tokenStatus3.getExMsg() != null) {
                System.out.println("getCloudToken ExMsg:" + tokenStatus.getExMsg());
                return;
            }
            
            String jsonOut3=tokenStatus3.getJson();
            System.out.println("JSONOUT3:" + jsonOut3);
            
            File createFile = new File(outFileName);
            TokenManager.getCloudData(nodeIOName, deliveryNode, tokenID, createFile, logger);
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();
            
        } finally {
            
            CloudResponse response = TokenManager.deleteCloudToken(nodeIOName, deliveryNode, tokenID, logger);
            System.out.println("Delete Status:" + response.getHttpStatus());
            
            
            CloudResponse response2 = TokenManager.deleteCloudData(nodeIOName, deliveryNode, tokenID, logger);
            System.out.println("Delete Data:" + response2.getHttpStatus());

        }
        
        
    }
    
    public static AsyncCloudArchive getAsyncCloudArchive(
            TokenStatus tokenStatus,
            LoggerInf logger)
        throws TException
    {
        return new AsyncCloudArchive(tokenStatus, logger);
    }
    
    protected AsyncCloudArchive(
            TokenStatus tokenStatus,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.tokenStatus = tokenStatus;
        validate();
        getCloudArchive();
    }
    private void validate()
        throws TException
    {
        
    }
    
    private void getCloudArchive()
        throws TException
    {
        
        try {
            String nodeIOName = this.tokenStatus.getNodeIOName();
            Long extractNode = this.tokenStatus.getExtractNode();
            extractAccessNode = NodeIO.getCloudNode(nodeIOName, extractNode, logger);
            CloudStoreInf extractService = extractAccessNode.service;
            String extractBucket = extractAccessNode.container;
            
            Long deliveryNode = this.tokenStatus.getDeliveryNode();
            deliveryAccessNode = NodeIO.getCloudNode(nodeIOName, deliveryNode, logger);
            
            String archiveTypeS = this.tokenStatus.getArchiveType().toString();
            workBase = FileUtil.getTempDir("archive");
            
            cloudArchive = new CloudArchive(
                    extractService, 
                    extractBucket,
                    tokenStatus.getObjectID(),
                    workBase,
                    archiveTypeS,
                    logger
            ).setDeleteFileAfterCopy(true);

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    @Override
    public void run()
    {
        try {
            log("run entered");
            String archiveName = tokenStatus.getToken();
            tokenStatus.currentBuildStart();
            switch (tokenStatus.getArchiveContent()) {
                case object:
                    Boolean full = this.tokenStatus.getFullObject();
                    archiveContent = cloudArchive.buildObject(archiveName, full);
                    break;
                    
                case version:
                    Integer versionID = this.tokenStatus.getVersionID();
                    archiveContent = cloudArchive.buildVersion(versionID, archiveName);
                    break;
                    
                case producer:
                    Integer producerVersionID = this.tokenStatus.getVersionID();
                    List<String> filterList = this.tokenStatus.getFilterList();
                    archiveContent = cloudArchive.buildProducer(producerVersionID, filterList, archiveName);
                    break;
            }
            
            File outputFile = archiveContent.getFile();
            if (!outputFile.exists() || (outputFile.length() <= 0)) {
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "Async file does not exist:"
                        + outputFile.getCanonicalPath());
            }
            System.out.println("File build:" 
                    + " - path:" + outputFile.getCanonicalPath() 
                    + " - length:" + outputFile.length()
            );
            String outKey = tokenStatus.getToken()  + "/data";
            tokenStatus.currentBuildEnd();
            saveCloud(outKey, outputFile, logger);
            System.out.println(MESSAGE + "Complete:" + tokenStatus.getToken());

        } catch (Exception ex) {
            String msgx = MESSAGE + "Exception for "
                    + " - Exception:" + ex
                    ;
            System.out.println("Exception: " + msgx);
            ex.printStackTrace();
            logger.logError(msgx, 2);

        } finally {
            // delete zip and base
            try {
                FileUtil.deleteDir(workBase);
            } catch (Exception ex) { }
        }

    }
    
    protected void saveCloud(
            String outKey,
            File outputFile,
            LoggerInf logger)
        throws TException
    {
        String bucket = null;
        try {
            if (deliveryAccessNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deliveryAccessNode missing and required");
            }
            
            CloudStoreInf service = deliveryAccessNode.service;
            bucket = deliveryAccessNode.container;
            CloudResponse response = service.putObject(bucket,outKey, outputFile);
            Exception exc = response.getException();
            if (exc != null) {
                System.out.println("saveCloud Exception:" + ex);
                throw exc;
            }
            tokenStatus.setTokenStatusEnum(TokenStatus.TokenStatusEnum.OK);
            
        } catch (Exception ex) {
            if (ex.toString().contains("REQUESTED_ITEM_NOT_FOUND") 
                    || ex.toString().contains("404")) {
                tokenStatus.setExStatus(404);
            } else {
                tokenStatus.setExStatus(500);
            }
            tokenStatus.setExMsg(ex.toString());
            tokenStatus.setTokenStatusEnum(TokenStatus.TokenStatusEnum.SERVICE_EXCEPTION);
            System.out.println("saveCloud after Exception"
                + " - bucket:" + bucket
                + " - outKey:" + outKey
                + " - Exception:" + tokenStatus.getExMsg()
                    );
        }
        
        //reset token/status
        try {
            
            Long deliveryNode = tokenStatus.getDeliveryNode();
            String nodeIOName = tokenStatus.getNodeIOName();
            String tokenID = tokenStatus.getToken();
            
            // delete for replace
            CloudResponse response = TokenManager.deleteCloudToken(nodeIOName, deliveryNode, tokenID, logger);
            
            TokenManager.saveCloudToken(nodeIOName, deliveryNode, tokenStatus, logger);
            
        } catch (Exception ex) {
            if (ex.toString().contains("REQUESTED_ITEM_NOT_FOUND") 
                    || ex.toString().contains("404")) {
                tokenStatus.setExStatus(404);
            } else {
                tokenStatus.setExStatus(500);
            }
            tokenStatus.setExMsg(ex.toString());
            tokenStatus.setTokenStatusEnum(TokenStatus.TokenStatusEnum.SERVICE_EXCEPTION);
        }
    }
    
    public void callEx()
        throws TException
    {
        run();
        throwEx();
    }
}

