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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.VersionMapUtil;
import org.cdlib.mrt.cloud.test.TestProducerArchive;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.store.KeyFileInf;
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
public class ProducerComponent
{
    
    protected static final String NAME = "ProducerComponent";
    protected static final String MESSAGE = NAME + ": ";
    private boolean DEBUG = true;
    
    protected HashMap<String, String> filterHash = null;
    protected LoggerInf logger = null;
                    
    /**
     * constructor
     * 
     * @param filterList list of  producer file names to be excluded from archive
     * @param logger Merritt logger
     * @throws TException 
     */
    public ProducerComponent(
            List<String> filterList,
            LoggerInf logger)
        throws TException
    {
        this.logger = logger;
        buildFilterHash(filterList);
    }
    
    protected void buildFilterHash(List<String> filterList)
    {
        if (filterList == null) {
            filterHash = null;
            return;
        }
        filterHash = new HashMap();
        for (String filterName : filterList) {
            filterHash.put(filterName, "p");
        }
    }
    
    public ArchiveComponent edit(ArchiveComponent archiveComponent)    
            throws TException
    {
        String PRODUCER = "producer";
        try {
            String fileName = archiveComponent.fileName;
            String filePath = archiveComponent.filePath;
            if (filePath.indexOf(PRODUCER) != 0) return null;
            fileName = fileFilter(fileName);
            if (fileName == null) return null;
            //return new ArchiveComponent(key, fileName, filePath);
            if (filePath.equals(PRODUCER)) {
                archiveComponent.filePath = "";
            } else {
                archiveComponent.filePath = filePath.substring(9); // producer/
            }
            return archiveComponent;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public String fileFilter(String fileName)
        throws TException
    {
        if (filterHash != null) {
            if (DEBUG) System.out.println("hash:"  + fileName + ":" + filterHash.get(fileName));
            String val = filterHash.get(fileName);
            if (val == null) return fileName;
            return null;
        }
        if (fileName.indexOf("mrt-") == 0) {
            return null;
        }
        return fileName;
    }
}
