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
import static org.cdlib.mrt.store.action.ComponentList.MESSAGE;
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
public class ProducerComponentList
    extends ArchiveComponentList
{
    
    protected static final String NAME = "ProducerComponentList";
    protected static final String MESSAGE = NAME + ": ";
    private boolean DEBUG = true;
    
    protected HashMap<String, String> filterHash = null;
                    
    /**
     * constructor
     * @param ark identifier of object to be added
     * @param version version id of producer content to be added: number of version or zero for current
     * @param archiveName name of base for expanded archive if base included
     * @param baseDir directory for building archive
     * @param versionMap object version map
     * @param filterList list of  producer file names to be excluded from archive
     * @param logger Merritt logger
     * @throws TException 
     */
    public ProducerComponentList(
            Identifier ark,
            int version,
            String archiveName,
            File baseDir,
            VersionMap versionMap,
            List<String> filterList,
            LoggerInf logger)
        throws TException
    {
        super(ark, version, archiveName, baseDir, versionMap, logger);
        buildFilterHash(filterList);
    }
    
    /**
     * proceess the building of the archive
     * @param getKey class containing  methode for getting content for a key
     * @param archiveType zip, tar, targz
     * @param includeArchiveBase true=include base directory in archive; false=skip base directory
     * @return FileContent for archive
     * @throws TException 
     */
    public FileContent process(KeyFileInf getKey, 
            String archiveType, 
            boolean includeArchiveBase)
        throws TException
    {
        try {
        
            edit();
            addFiles(getKey);
            FileContent producerArchive = buildArchive(includeArchiveBase, archiveType);
            //FileContent producerArchive = buildArchive(false, archiveType);
            System.out.println(producerArchive.dump("test"));
            deleteArchiveDir();
            return producerArchive;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void process(KeyFileInf getKey, 
            String archiveType, 
            boolean includeArchiveBase,
            OutputStream outputStream
    )
        throws TException
    {
        try {
        
            edit();
            addFiles(getKey);
            buildArchive(includeArchiveBase, outputStream, archiveType);
            deleteArchiveDir();
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void buildFilterHash(List<String> filterList)
    {
        if (filterList == null) {
            filterHash = null;
        }
        filterHash = new HashMap();
        for (String filterName : filterList) {
            filterHash.put(filterName, "p");
        }
    }
    
    protected ArchiveComponent doEdit(ArchiveComponent archiveComponent)    throws TException
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
