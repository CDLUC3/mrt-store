/*
Copyright (c) 2005-2010, Regents of the University of California
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


import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.store.TokenGetState;
import org.cdlib.mrt.store.TokenStatus;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.store.logging.LogEntryTokenStatus;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.json.JSONObject;
/**
 * TokenRun
 * Routine to create an Asynch container file and save to cloud
 * <pre>
 * Zookeeper request is of the form:
 * {
 *   "status":201,
 *   "token":"8423294b-8cb7-49e5-b42b-2a4a6f3596e2",
 *   "cloud-content-byte":206290233,
 *   "delivery-node":7001
 * }
 * 
 * status - http status of queue process
 * token - async token used for retrieval
 * cloud-content-byte - size
 * delivery-node - cloud for storage of container file
 * 
 * Run process
 *          //zooEnter zookeeper entry
 *          //storageConfig general storage configuration
 *          TokenRun tokenRun = getTokenRun(zooEnter, storageConfig);
 *          tokenRun.run();
 *          if (tokenRun.getRunStatus() != TokenRunStatus.OK) {
 *               System.out.println("TokenRun not OK:" + tokenRun.getRunStatus());
 *               if (tokenRun.getException() != null) {
 *                   System.out.println("Exception:" + tokenRun.getException();
 *          }
 * 
 * Response:
 *    tokenRun.getRunStatus()
 *    TokenRunStatus.OK = run completed successfully
 *    TokenRunStatus.Ready = In ready state before archive processing
 *    TokenRunStatus.Processing = currently building archive
 *    TokenRunStatus.Error = error occurred in processing
 * </pre>
 * @author dloy
 */
public class TokenRun
    implements Runnable
{
    // Presigned errors
    public enum TokenRunStatus {OK, Ready, Processing, Missing, Retry, Error};

    protected static final String NAME = "TokenRun";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String NL = System.getProperty("line.separator");

    protected LoggerInf logger = null;
    protected StorageConfig storageConfig = null;
    protected JSONObject queueRequest = null;
    
    protected JSONObject result = null;
    
    protected Long deliveryNode = null;
    protected Long cloudContentBytes = null;
    protected String asynchToken = null;
    protected TokenManager tokenManager = null;
    
    protected TokenStatus startTokenStatus = null;
    protected TokenStatus processTokenStatus = null;
    protected TokenStatus endTokenStatus = null;
    protected TokenRunStatus runStatus = null;
    protected Exception exception = null;
    protected TokenGetState getState = null;
    protected Long processTimeMs = null;
    protected long buildMs = 0;
    protected long buildFileCnt = 0;
        
    
    protected NodeIO nodeIO = null;
    
    /**
     * Get TokenRun for processing Async container file
     * @param queueRequest zoo token json of the form:{"status":201,"token":"8423294b-8cb7-49e5-b42b-2a4a6f3596e2","cloud-content-byte":206290233,"delivery-node":7001,"message":"Request queued, use token to check status"}
     * @param storageConfig storage configuration routine
     * @return constructed TokenRun
     * @throws TException 
     */
    public static TokenRun getTokenRun(String queueRequestS, StorageConfig storageConfig)
        throws TException
    {
        try {
            JSONObject queueRequest = new JSONObject(queueRequestS);
            return  new TokenRun(queueRequest, storageConfig);
            
        
                    
        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
    }
    
    public TokenRun(JSONObject queueRequest, StorageConfig storageConfig)
        throws TException
    {
        try {
            this.storageConfig = storageConfig;
            this.logger = storageConfig.getLogger();
            extract(queueRequest);
            nodeIO = storageConfig.getNodeIO();
            if (nodeIO == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "NodeIO null");
            }
            NodeIO.AccessNode deliveryAccessNode = nodeIO.getAccessNode(deliveryNode);
            if (deliveryAccessNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "NodeIO node not found:" + deliveryNode);
            }
            setTokenStatus();
            
        } catch (Exception ex) {
            exception = ex;
            runStatus = TokenRunStatus.Error;
            System.out.println("TokenRun exception saved:" + ex);
        }
    } 
    
    
    public void extract(JSONObject queueRequest)
        throws TException
    {
        try {
            cloudContentBytes = queueRequest.getLong("cloud-content-byte");
            deliveryNode = queueRequest.getLong("delivery-node");
            asynchToken = queueRequest.getString("token");
                    
        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    
    protected void setTokenStatus()
        throws TException
    {
        tokenManager = TokenManager.getExistingTokenManager(nodeIO, deliveryNode, asynchToken, logger);
        if (tokenManager == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Token manager null");
        }
        startTokenStatus = getCloudTokenStatus();
        processTokenStatus = getCloudTokenStatus();
        if (DEBUG) System.out.println(startTokenStatus.dump("TokenRun.setTokenStatus(start)"));
        if (DEBUG) System.out.println(processTokenStatus.dump("TokenRun.setTokenStatus(process)"));
    }
    
    
    
    public void test()
    {
        String out = null;
        try {
            TokenGetState getState = TokenGetState.getTokenGetState(processTokenStatus);
            out = getState.getJson();
            System.out.println("no error:" + out);
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
        }
        
    }
    
    
    public void run()
    {
        try {
            if (runStatus == TokenRunStatus.Error) {
                System.out.println("Failed to start run");
                return;
            }
            TokenRunStatus status = testStartStatus();
            if (status == TokenRunStatus.Ready) {
                buildTokenContainer();
                
            } else {
                System.out.println("buildTokenContainer not called - status:" + status.toString());
            }
            if (endTokenStatus != null) {
                getState = TokenGetState.getTokenGetState(endTokenStatus);
            }
            
        } catch (Exception ex) {
            exception = ex;
            setRunStatus(TokenRunStatus.Error);
            
        } finally {
            if (logger != null) {
                logger.logMessage(MESSAGE + "Run Status - " + getRunStatus()
                        + " - deliveryNode:" + deliveryNode
                        + " - asynchToken:" + asynchToken
                        + " - exception:" + exception
                        , 3, true);
            }
        }
    }
    
    public TokenRunStatus testStartStatus()
        throws TException
    {
        try {
            
            if (startTokenStatus == null) {
                setRunStatus(TokenRunStatus.Missing);
                return runStatus;
            }
            
            TokenStatus.TokenStatusEnum startStatusEnum = startTokenStatus.getTokenStatusEnum();
            
            if (startStatusEnum == TokenStatus.TokenStatusEnum.OK) {
                endTokenStatus = startTokenStatus;
                setRunStatus(TokenRunStatus.OK);
                return runStatus;
                
            } else if (startStatusEnum  == TokenStatus.TokenStatusEnum.SERVICE_EXCEPTION) {
                System.out.println("***RETRY ATTEMPTED:" + startTokenStatus.getToken());
            
            } else if (startStatusEnum  != TokenStatus.TokenStatusEnum.Queued) {
                endTokenStatus = startTokenStatus;
                String errMsg = "initial state not supported:" + startStatusEnum.toString();
                throw new TException.INVALID_ARCHITECTURE(errMsg);
            }
            setRunStatus(TokenRunStatus.Ready);
            
        } catch (Exception ex) {
            exception = ex;
            setRunStatus(TokenRunStatus.Error);
            
        } finally {
            return runStatus;
        }
    }
    
    public void buildTokenContainer()
        throws TException
    {
        
        try {
            setRunStatus(TokenRunStatus.Processing);
            processTokenStatus.setTokenStatusEnum(TokenStatus.TokenStatusEnum.NotReady);
            processTokenStatus.setExMsg(null);
            saveCloudTokenStatus(processTokenStatus);
            buildTokenSynch();
            if (processTokenStatus.getTokenStatusEnum() == TokenStatus.TokenStatusEnum.SERVICE_EXCEPTION) {
                saveCloudTokenStatus(processTokenStatus);
                return;
            }
            endTokenStatus = getCloudTokenStatus();
            TokenStatus.TokenStatusEnum endStatusEnum = endTokenStatus.getTokenStatusEnum();
            if (endStatusEnum == TokenStatus.TokenStatusEnum.OK) {
                setRunStatus(TokenRunStatus.OK);            // Add log state
                processTokenStatus.setBuildFileCnt(buildFileCnt);
                processTokenStatus.setBuildTimeMs(buildMs);
                LogEntryTokenStatus entry = LogEntryTokenStatus.getLogEntryTokenStatus(processTokenStatus);
                entry.addEntry();
                return;
            }
            
            String errMsg = "final state not OK:" + endStatusEnum.toString();
            throw new TException.INVALID_ARCHITECTURE(errMsg);
            
            
            
        } catch (TException tex) {
            setRunStatus(TokenRunStatus.Error);
        }
    }
    
    protected void buildTokenSynch()
        throws TException
    {
        try {
            long startTime = System.currentTimeMillis();
            AsyncCloudArchive asyncCloudArchive = AsyncCloudArchive.getAsyncCloudArchive(
                storageConfig.getNodeIO(),
                processTokenStatus,
                logger);
            asyncCloudArchive.run();
            processTimeMs = System.currentTimeMillis() - startTime;
            buildMs = asyncCloudArchive.getBuildMs();
            buildFileCnt = asyncCloudArchive.getBuildFileCnt();
            if (asyncCloudArchive.ex != null) {
                System.out.println("buildTokenSynch Exception:" + asyncCloudArchive.ex);
                asyncCloudArchive.ex.printStackTrace();
            }
            
        } catch (TException tex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(tex));
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
     * If debug flag on then sysout this message
     * @param msg message to sysout
     */
    protected void log(String msg)
    {
        if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
        //logger.logMessage(msg, 0, true);
    }
    
    public void dump(String header)
    {
        System.out.println("***DUMP TokenRun:" + header);
        System.out.println("Status:" + runStatus.toString());
        if (exception != null) {
            System.err.println("Exception ex:" + exception);
            exception.printStackTrace();
            return;
        }
        if (startTokenStatus != null) {
            System.out.println(startTokenStatus.dump("start"));
        }
        if (processTokenStatus != null) {
            System.out.println(processTokenStatus.dump("process"));
        }
        if (endTokenStatus != null) {
            System.out.println(endTokenStatus.dump("end"));
        }
        if (getState != null) {
            System.out.println("getState:" + getState.getJson());
        }
        if (processTimeMs != null) {
            System.out.println("processTimeMs:" + processTimeMs);
        }
    }

    public TokenStatus getStartTokenStatus() {
        return startTokenStatus;
    }

    public TokenStatus getEndTokenStatus() {
        return endTokenStatus;
    }

    public void setEndTokenStatus(TokenStatus endTokenStatus) {
        this.endTokenStatus = endTokenStatus;
    }

    public TokenRunStatus getRunStatus() {
        return runStatus;
    }
    
    public String getRunStatusValue() {
        if (runStatus == null) {
            return "none";
        }
        return runStatus.toString();
    }

    public Exception getException() {
        return exception;
    }

    public Long getProcessTimeMs() {
        return processTimeMs;
    }

    
    public void setRunStatus(TokenRunStatus runStatus) {
        if (DEBUG) {
            System.out.println("set runStatus:" + runStatus.toString());
            try {
                throw new Exception();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        this.runStatus = runStatus;
    }
    
    protected TokenStatus getCloudTokenStatus()
        throws TException
    {
        return tokenManager.getCloudToken(nodeIO, deliveryNode, asynchToken, logger);
    }
    
    protected String saveCloudTokenStatus(TokenStatus saveTokenStatus)
        throws TException
    {
        NodeIO nodeIO = storageConfig.getNodeIO();
        NodeIO.AccessNode deliveryAccessNode = nodeIO.getNode(processTokenStatus.getDeliveryNode());
        return tokenManager.saveCloudToken(deliveryAccessNode, saveTokenStatus, logger);
    }


    public static void main(String[] args) 
        throws TException
    {
        main_run(args);
        
    }

    public long getBuildMs() {
        return buildMs;
    }

    public long getBuildFileCnt() {
        return buildFileCnt;
    }
    
    public static void main_testGet(String[] args) 
        throws TException
    {
        StorageConfig storageConfig = null;
        String key = null;
        try {
            storageConfig = StorageConfig.useYaml();
            //String zooEnter = "{\"status\":201,\"token\":\"bf6ef133-9b05-4fd8-a4d1-845c40125315\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"b3aaa27d-b085-4173-835c-df4c9d65cd24\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}'";
            //String zooEnter = "{\"status\":201,\"token\":\"68b694d7-7571-41d1-94f3-dd2fb83d3506\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"03b87756-9f02-4abe-a78e-fe8c3025e9cd\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"8c605d34-46d4-466b-a0cd-1f1a117c52fd\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"4f0d1679-06d2-49e9-8751-5f3d77cbe25d\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            String zooEnter = "{\"status\":201,\"token\":\"34e36968-c713-487f-9492-ebc118627caf\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            TokenRun tokenRun = getTokenRun(zooEnter, storageConfig);
            if (tokenRun.getException() != null) {
                System.out.println("constructor Exception ex:" + tokenRun.getException());
                return;
            }
            tokenRun.test();

        } catch (TException tex) {
            System.out.println("tex:" + tex);
            tex.printStackTrace();
            
        } catch (Exception ex) {
            System.out.println("ex:" + ex);
            ex.printStackTrace();
            
        } finally {
        }
        
    }
    

    public static void main_run(String[] args) 
        throws TException
    {
        StorageConfig storageConfig = null;
        String key = null;
        try {
            storageConfig = StorageConfig.useYaml();
            //String zooEnter = "{\"status\":201,\"token\":\"bf6ef133-9b05-4fd8-a4d1-845c40125315\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"b3aaa27d-b085-4173-835c-df4c9d65cd24\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}'";
            //String zooEnter = "{\"status\":201,\"token\":\"68b694d7-7571-41d1-94f3-dd2fb83d3506\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"03b87756-9f02-4abe-a78e-fe8c3025e9cd\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"8c605d34-46d4-466b-a0cd-1f1a117c52fd\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"4f0d1679-06d2-49e9-8751-5f3d77cbe25d\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
       
            //String zooEnter = "{\"status\":201,\"token\":\"34e36968-c713-487f-9492-ebc118627caf\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            //String zooEnter = "{\"status\":201,\"token\":\"5ee14a65-a288-4cf8-a135-d6e326429f4c\",\"cloud-content-byte\":206290233,\"delivery-node\":7001,\"message\":\"Request queued, use token to check status\"}";
            
            String zooEnter = "{\"status\":201,\"token\":\"8423294b-8cb7-49e5-b42b-2a4a6f3596e2\",\"cloud-content-byte\":206290233,\"delivery-node\":7001}";
            TokenRun tokenRun = getTokenRun(zooEnter, storageConfig);
            tokenRun.run();
            if (tokenRun.getRunStatus() != TokenRunStatus.OK) {
                System.out.println("TokenRun not OK:" + tokenRun.getRunStatus());
            }
            if (tokenRun.getException() != null) {
                System.out.println("Throw Exception:" + tokenRun.getException());
                throw tokenRun.exception;
            }
            tokenRun.dump("test it");
            System.out.println("Run status value:"  + tokenRun.getRunStatusValue());

        } catch (TException tex) {
            System.out.println("tex:" + tex);
            tex.printStackTrace();
            
        } catch (Exception ex) {
            System.out.println("ex:" + ex);
            ex.printStackTrace();
            
        } finally {
        }
        
    }
    

}