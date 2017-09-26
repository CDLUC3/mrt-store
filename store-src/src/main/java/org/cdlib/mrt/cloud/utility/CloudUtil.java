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
import java.util.Properties;

import org.cdlib.mrt.store.cloud.CloudObjectService;

import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.aws.AWSS3Cloud;
//import org.cdlib.mrt.s3.sdsc.SDSCCloud;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.NodeIO;

    
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
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "unable to load AWS Service logicalVolume required");
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
        
}
