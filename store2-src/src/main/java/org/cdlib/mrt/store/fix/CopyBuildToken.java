
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
import java.util.Properties;
import java.util.Set;

import org.json.JSONObject;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.tools.CloudManifestCopy;
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
public class CopyBuildToken
        extends CloudFixAbs
{
    
    protected static final String NAME = "CopyBuildToken";
    protected static final String MESSAGE = NAME + ": ";
    private boolean DEBUG = false;
    protected JSONObject jsonResponse = null;
    protected boolean runS3 = false;
    protected CloudManifestCopy copy = null;
    protected BuildTokenCC build = null;
    
    protected int version = 0;
    protected Long nodeIDBackup = null;
    protected NodeIO.AccessNode inService = null;
    protected NodeIO.AccessNode outService = null;
    protected CloudStoreInf in = null;
    protected CloudStoreInf out = null;
    protected String inContainer = null;
    protected String outContainer = null;
    protected String manKey = null;
    
                    
    public CopyBuildToken(
            NodeIO nodeIO, 
            Long nodeID, 
            Long nodeIDBackup, 
            Identifier objectID, 
            LoggerInf logger)
        throws TException
    {
        super(nodeIO, nodeID, objectID, logger);
        this.nodeIDBackup = nodeIDBackup;
        inService = nodeIO.getAccessNode(nodeID);
        outService = nodeIO.getAccessNode(nodeIDBackup);
        build = new BuildTokenCC(nodeIO, nodeIDBackup, objectID, logger); // this does build on copied version
        setCopy();
        String msg = MESSAGE
                + " - nodeID=" + this.nodeID  
                + " - inBucket=" + inService.container
                + " - nodeIDBackup=" + this.nodeIDBackup
                + " - outBucket=" + outService.container
                + " - objectID="+ this.objectID.getValue()
                + " - currentVersion=" + this.objectCurrent;
        System.out.println(msg);
    }
    
    private void setCopy()
        throws TException
    {
        try {
            if (inService == null) {
                throw new TException.INVALID_OR_MISSING_PARM("inService missing");
            }
            if (outService == null) {
                throw new TException.INVALID_OR_MISSING_PARM("outService missing");
            }
            manKey = objectID.getValue() + "|manifest";
            in = inService.service;
            out = outService.service;
            inContainer = inService.container;
            outContainer = outService.container;

            copy = new CloudManifestCopy(in, inContainer, out, outContainer, logger);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void process()
       throws TException
    {
        try {
            long start = System.currentTimeMillis();
            if (!exists()) {
                copy.copyObject(objectID.getValue());
            }
            long mlsComplete = System.currentTimeMillis() - start;
            System.out.println("copy completion time:"
                    + " - objectID=" + objectID.getValue()
                    + " - millisec=" + mlsComplete
            );
            build.process();
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public boolean exists()
       throws TException
    {
        boolean match = false;
        try {
            
            Properties outProp = out.getObjectMeta(outContainer, manKey);
            if ((outProp != null) && (outProp.size() > 0)) {
                Properties inProp = in.getObjectMeta(inContainer, manKey);
                String inSha256 = inProp.getProperty("sha256");
                String outSha256 = outProp.getProperty("sha256");
                if (inSha256.equals(outSha256)) {
                    System.out.println("Copy object exists");
                    return true;
                }
            }
            return false;
            //build.process();
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
}
