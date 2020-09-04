package org.cdlib.mrt.store.action;

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
import java.util.List;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.VersionMapUtil;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudUtil;
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
public class ComponentList {
    
    protected static final String NAME = "ComponentListManifest";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    
    protected Identifier ark = null;
    protected VersionMap versionMap = null;
    protected LoggerInf logger = null;
                    
    public ComponentList(
            Identifier ark,
            VersionMap versionMap,
            LoggerInf logger)
        throws TException
    {
        this.ark = ark;
        this.versionMap = versionMap;
        this.logger = logger;
    }
    
    public CloudList buildCloudList(int version)
        throws TException
    {
        
        try {
            CloudList cloudList = new CloudList();
            int current = versionMap.getCurrent();
                System.out.println("***Current:" + current);
            //addManifest(arkS, cloudList);
            if (version < 0) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "version number invalid: " + version);
                
            } else if (version > current) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "requested version > current:"
                + " - request:" + version
                + " - current:" + current
                );
                
            } else if (version == 0) {
                buildCloudListExt(cloudList, current);
                
            } else {
                buildCloudListExt(cloudList, version);
            }
            return cloudList;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void buildCloudListExt(CloudList cloudList, int version)
        throws TException
    {
        
        try {
            if (DEBUG) System.out.println("Version:" + version);
            List<FileComponent> versionList = VersionMapUtil.getVersion(versionMap, version);
            if (versionList == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "no versionList found:"
                        + " - ark:" + ark.getValue()
                        + " - Version:" + version
                );
            }
            for (FileComponent component : versionList) {
                String key = component.getLocalID();
                if (DEBUG) System.out.println("key:" + key);
                MessageDigest digest = component.getMessageDigest();
                CloudUtil.KeyElements keyElements = CloudUtil.getKeyElements(key);
                //CloudResponse response = inService.getObjectList(inContainer, key);
                //if (keyElements.versionID != v) continue;
                //Need to allow zero length files - if (component.getSize() == 0) continue;
                CloudList.CloudEntry entry = new CloudList.CloudEntry()
                        .sKey(key)
                        .sSize(component.getSize())
                        .sContentType(component.getMimeType())
                        .sDigest(digest)
                        .sVersion(version)
                        .sCreated(component.getCreated());
                cloudList.add(entry);
            }
            
        } catch (TException me) {
            me.printStackTrace();
            throw me;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
}
