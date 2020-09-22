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
package org.cdlib.mrt.store.action;

import java.io.File;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TException;
/**
 * Used for building archive directory
 * @author dloy
 */
public class ArchiveComponent 
{
    private static final String NAME = "ArchiveComponent";
    private static final String MESSAGE = NAME + ": ";
    private static boolean  DEBUG = false;
    public String key = null;
    public Identifier objectID = null;
    public int version = 0;
    public String filePath = null;
    public String fileName = null;
    public File archiveFile = null;
    public CloudList.CloudEntry cloudEntry = null;
        
    public static ArchiveComponent fromKey(String key)
        throws TException
    {
        try {
            String parts[] = key.split("\\|",3);
            if (parts.length < 3) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "bad key:" + key);
            }
            Identifier objectID = new Identifier(parts[0]);
            String versionS = parts[1];
            int version = Integer.parseInt(versionS);
            String fileID = parts[2];
            //System.out.println("fileID:" + fileID);
            
            String fileName = null;
            String filePath = null;
            int pos = fileID.lastIndexOf('/');
            if (pos < 0) return null;
            
            fileName = fileID.substring(pos + 1);
            filePath = fileID.substring(0, pos);
            //return new ArchiveComponent(key, fileName, filePath);
            ArchiveComponent archiveComponent = new ArchiveComponent()
                    .setKey(key)
                    .setObjectID(objectID)
                    .setVersion(version)
                    .setFileName(fileName)
                    .setFilePath(filePath);
            if (DEBUG) System.out.println("fileID:" + fileID
                    + " - fileName=" + fileName
                    + " - filePath=" + filePath
            );
            
            return archiveComponent;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public ArchiveComponent() { }

    public ArchiveComponent(String key, String filePath, String fileName)
    {
        this.key = key;
        this.filePath = filePath;
        this.fileName = fileName;
    }

    public String getKey() {
        return key;
    }

    public ArchiveComponent setKey(String key) {
        this.key = key;
        return this;
    }

    public String getFilePath() {
        return filePath;
    }

    public ArchiveComponent setFilePath(String filePath) {
        this.filePath = filePath;
        return this;
    }

    public Identifier getObjectID() {
        return objectID;
    }

    public ArchiveComponent  setObjectID(Identifier objectID) {
        this.objectID = objectID;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public ArchiveComponent setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getFileName() {
        return fileName;
    }

    public ArchiveComponent setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public File getArchiveFile() {
        return archiveFile;
    }

    public ArchiveComponent setArchiveFile(File archiveFile) {
        this.archiveFile = archiveFile;
        return this;
    }

    public CloudList.CloudEntry getCloudEntry() {
        return cloudEntry;
    }

    public ArchiveComponent setCloudEntry(CloudList.CloudEntry cloudEntry) {
        this.cloudEntry = cloudEntry;
        return this;
    }
    
}
