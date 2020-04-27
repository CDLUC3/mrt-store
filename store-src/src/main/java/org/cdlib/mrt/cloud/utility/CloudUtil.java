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
package org.cdlib.mrt.cloud.utility;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import javax.ws.rs.core.Response;

import org.cdlib.mrt.store.cloud.CloudObjectService;

import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.aws.AWSS3Cloud;
//import org.cdlib.mrt.s3.sdsc.SDSCCloud;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.store.PreSignedState;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.URLEncoder;

    
public class CloudUtil
{
    protected static final String NAME = "CloudUtil";
    protected static final String MESSAGE = NAME + ": ";
    protected static boolean DEBUG = true;
    
    /*
    public static CloudObjectService getSDSCService(LoggerInf logger, Properties xmlProp, String bucket)
        throws TException
    {
        try {
            CloudStoreInf s3service = SDSCCloud.getSDSC(xmlProp, logger);  
            return CloudObjectService.getCloudObjectState(s3service, bucket, logger);

            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to build getSDSCService");
        }
    }
    
    public static CloudObjectService getSDSCService(LoggerInf logger,  String bucket)
        throws TException
    {
        try {
            String loadName = "resources/SDSC-S3.properties";
            ManInfo test = new ManInfo();
            InputStream propStream =  test.getClass().getClassLoader().
                getResourceAsStream(loadName);
            if (propStream == null) {
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to load " + loadName);
            }
            Properties xmlProp = new Properties();
            xmlProp.load(propStream);
            return getSDSCService(logger, xmlProp, bucket);

            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to build getSDSCService", ex);
        }
    }
    
    */
    public static CloudObjectService getOpenstackService(LoggerInf logger, Properties xmlProp, String bucket)
        throws TException
    {
        try {
            CloudStoreInf s3service = OpenstackCloud.getOpenstackCloud(xmlProp, logger);  
            return CloudObjectService.getCloudObjectState(s3service, bucket, logger);

            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to build getSDSCService");
        }
    }
    
    public static CloudObjectService getOpenstackService(LoggerInf logger,  String bucket)
        throws TException
    {
        try {
            String loadName = "resources/SDSC-S3.properties";
            ManInfo test = new ManInfo();
            InputStream propStream =  test.getClass().getClassLoader().
                getResourceAsStream(loadName);
            if (propStream == null) {
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to load " + loadName);
            }
            Properties xmlProp = new Properties();
            xmlProp.load(propStream);
            return getOpenstackService(logger, xmlProp, bucket);

            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to build getSDSCService", ex);
        }
    }
    
    public static CloudObjectService getAWSService(LoggerInf logger,  String logicalVolume)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(logicalVolume)) {
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to load AWS Service logicalVolume required");
            }               
            String [] parts = logicalVolume.split("\\|");
            String bucketName = null;
            String storageClass = null;
            if (parts.length == 1) {
                bucketName = parts[0];
                storageClass = "Standard";
                
            } else if (parts.length == 2) {
                bucketName = parts[0];
                storageClass = parts[1];
                
            } else {
                System.out.println("logicalVolume=" + logicalVolume + " - length=" + parts.length);
                for (String part : parts) {
                    System.out.println("part=" + part);
                }
                throw new TException.INVALID_ARCHITECTURE(MESSAGE 
                        + "getAWSService requires that logicalVolume contain either bucketName or bucketName|storageClass:" 
                        + logicalVolume);
            }
            if (DEBUG) System.out.println(MESSAGE +  "getAWSService:"
                    + " - bucketName=" + bucketName
                    + " - storageClass=" + storageClass
            );
            CloudStoreInf s3service =  AWSS3Cloud.getAWSS3(storageClass, logger);
            return CloudObjectService.getCloudObjectState(s3service, bucketName, logger);

            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to build getSDSCService", ex);
        }
    }
    
    public static CloudObjectService getNodeIOService(LoggerInf logger,  String logicalVolume)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(logicalVolume)) {
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to load NodeIO Service logicalVolume required");
            }               
            String [] parts = logicalVolume.split("\\|");
            String nodeIOName = null;
            long nodeNumber = 0;
            if (parts.length != 2) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE 
                        + "NodeIO services requires nodeIOName|node number:" 
                        + logicalVolume);
            } 
            nodeIOName = parts[0];
            String nodeS = parts[1];
            try {
                nodeNumber = Long.parseLong(nodeS);
            } catch (Exception ex) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE 
                        + "NodeIO services requires numeric node value:" + nodeS);
                
            }
            NodeIO.AccessNode accessNode = NodeIO.getCloudNode(nodeIOName, nodeNumber, logger);
            if (accessNode == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE 
                        + "NodeIO AccessNode not found for:"
                        + " - nodeIOName=" + nodeIOName
                        + " - nodeNumber=" + nodeNumber
                );
            }
            CloudStoreInf cloudService =  accessNode.service;
            return CloudObjectService.getCloudObjectState(cloudService, accessNode.container, logger);

            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to build getSDSCService", ex);
        }
    }
    
    public static CloudObjectService getFileCloudService(LoggerInf logger,  String bucket)
        throws TException
    {
        try {
            CloudStoreInf fileCloudService = PairtreeCloud.getPairtreeCloud(true, logger);
            return CloudObjectService.getCloudObjectState(fileCloudService, bucket, logger);

            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to build getSDSCService", ex);
        }
    }
    
    /**
     * Return PreSignedState containing both presigned URL and any error conditions
     * @param nodeIOName nodeIO Table Name containing cloud information for access
     * @param nodeID node in nodeIO Table
     * @param key key to be looked up
     * @param expireMinutes expiration Minutes
     * @param logger
     * @return PreSigenedState
     * @throws TException routine attempts to capture all errors
     */
    public static PreSignedState getPreSignedURI(
            String nodeIOName,
            int nodeID,
            String key,
            Long expireMinutes,
            String contentType,
            String contentDisp,
            LoggerInf logger)
        throws TException
    {
        PreSignedState state = PreSignedState.getPreSignedState();
        if (expireMinutes == null) {
            expireMinutes = 240L;
        }
        try {
            System.out.println("getPreSigned entered:"
                    + " - nodeIOName=" + nodeIOName
                    + " - nodeId=" + nodeID
                    + " - key=" + key
                    + " - expireMinutes=" + expireMinutes
                    + " - contentType=" + contentType
                    + " - contentDisp=" + contentDisp
                    );
            
    
            if (nodeIOName == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "signedNodeName properties missing");
            };
            
            NodeIO nodeIO = new NodeIO(nodeIOName, logger);
            long outNode = nodeID;
            
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(outNode);
            if (accessNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "NodeIO.AccessNode not found - outNode:" + outNode);
            }
            
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            CloudResponse response = service.getPreSigned(expireMinutes, bucket, key, contentType, contentDisp);
            Exception ex = response.getException();
            if (ex != null) {
                System.out.println("ex:" + ex);
                
                // unimplemented returns the storage link for redirect
                if (ex instanceof TException.UNIMPLEMENTED_CODE) {
                    state.setStatusEnum(PreSignedState.StatusEnum.UNSUPPORTED_FUNCTION);
                    return state;
                
                // error
                } else if (ex instanceof TException.REQUESTED_ITEM_NOT_FOUND) {
                    state.setStatusEnum(PreSignedState.StatusEnum.REQUESTED_ITEM_NOT_FOUND);
                    return state;
                    
                } else if (ex instanceof TException.REQUEST_ITEM_EXISTS) {
                    state.setStatusEnum(PreSignedState.StatusEnum.OFFLINE_STORAGE);
                    return state;
                    
                } else {
                    state.setStatusEnum(PreSignedState.StatusEnum.SERVICE_EXCEPTION);
                    state.setEx(ex);
                    return state;
                }
                
            // signed URL returned
            } else {
                state.setStatusEnum(PreSignedState.StatusEnum.OK);
                URL signed = response.getReturnURL();
                state.setUrl(signed);
                state.setExpires(expireMinutes);
            }
            return state;
            
        } catch (TException tex) {
            state.setStatusEnum(PreSignedState.StatusEnum.SERVICE_EXCEPTION);
            state.setEx(tex);
            return state;

        } catch (Exception ex) {
            state.setStatusEnum(PreSignedState.StatusEnum.SERVICE_EXCEPTION);
            state.setEx(ex);
            return state;
        }
    }
        
}
