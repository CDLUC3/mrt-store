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
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.store.KeyFileInf;
import org.cdlib.mrt.utility.ArchiveBuilderBase;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.FixityTests;
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
public class ArchiveComponentList
    extends ComponentList
{
    
    protected static final String NAME = "ArchiveComponentList";
    protected static final String MESSAGE = NAME + ": ";
    private boolean DEBUG = false;
    
    protected Tika tika = null;
    protected int version = 0;
    protected CloudList fullCloudList = null;
    protected ArrayList<ArchiveComponent> editComponents = null;
    protected File archiveDir = null;
    protected File baseDir = null;
    protected String archiveName = null;
                    
    public ArchiveComponentList(
            Identifier ark,
            int version,
            String archiveName,
            File baseDir,
            VersionMap versionMap,
            LoggerInf logger)
        throws TException
    {
        super(ark, versionMap, logger);
        if (versionMap == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "versionMap- required");
        }
        if (versionMap.getCurrent()  == 0) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND(
                    MESSAGE + "versionMap empty:"
                    + " - ark:" + ark.getValue()
            );
        }
        if (ark == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "ark - required");
        }
        if (archiveName == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "archiveName - required");
        }
        this.archiveName = archiveName;
        if (baseDir == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "baseDir - required");
        }
        this.baseDir = baseDir;
        this.fullCloudList = buildCloudList(version);
        tika = Tika.getTika(logger);
    }
    
    public List<ArchiveComponent> edit()
        throws TException
    {
        List<CloudList.CloudEntry> entryList = fullCloudList.getList();
        editComponents = new ArrayList<>();
        try {
            for (CloudList.CloudEntry entry: entryList) {
                ArchiveComponent editComponent = getArchiveComponent(entry);
                if (editComponent != null) {
                    editComponents.add(editComponent);
                }
                
            }
            return editComponents;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected ArchiveComponent getArchiveComponent(CloudList.CloudEntry inEntry)
        throws TException
    {
        String key = inEntry.getKey();
        try {
            String parts[] = key.split("\\|",3);
            if (parts.length < 3) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "bad key:" + key);
            }
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
                    .setFileName(fileName)
                    .setFilePath(filePath)
                    .setCloudEntry(inEntry);
            if (DEBUG) System.out.println("fileID:" + fileID
                    + " - fileName=" + fileName
                    + " - filePath=" + filePath
                    + " - len=" + inEntry.getSize()
            );
            return  doEdit(archiveComponent);
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected ArchiveComponent doEdit(ArchiveComponent archiveComponent)    
            throws TException
    {
        return archiveComponent;
    }
    
    public void addFiles(KeyFileInf content)
        throws TException
    {
        try {
            if (baseDir == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "baseDir - file required");
            }
            this.archiveDir = new File(baseDir, archiveName);
            archiveDir.mkdir();
            
            FileUtil.deleteDir(archiveDir);
            addDir(archiveDir);
            for (ArchiveComponent component : editComponents) {
                addFile(component, archiveDir, content);
            }

        } catch (TException tex) {
            System.out.println(MESSAGE + "TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    protected void addFile(ArchiveComponent component, File archiveDir, KeyFileInf content)
        throws TException
    {
        try {
            File componentDir = archiveDir;
            if (component.filePath.length() > 0) {
                componentDir = new File(archiveDir, component.filePath);
                addDir(componentDir);
            }
            component.archiveFile = new File(componentDir, component.fileName);
            if (content != null) {
                content.keyToFile(component.key, component.archiveFile);
            }

        } catch (TException tex) {
            System.out.println(MESSAGE + "TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected void addDir(File dir)
        throws TException
    {
        boolean direxists = false;
        try {
            if (dir == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "addDir - file required");
            }
            try {
                direxists = dir.exists();
                if (!direxists) {
                    direxists = dir.mkdirs();
                }
            } catch (Exception ex)  {}
            if (!direxists) {
                throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "addDir - unable to create path:" + dir.getAbsolutePath());
            }

        } catch (TException tex) {
            System.out.println(MESSAGE + "TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public File getArchiveDir() {
        return archiveDir;
    }
    
    /**
     * Get an archive file from a specific version
     * @param versionFile version directory file to return as full archive
     * @param archiveType type of archive (tar, targz, zip)
     * @return Archive file and state info of archive file
     * @throws TException process exception
     */
    public FileContent buildArchive(
            boolean includeBase,
            String archiveTypeS)
        throws TException
    {
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilderBase.ArchiveType archiveType = null;
        try {
            archiveType
                    = ArchiveBuilderBase.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }
        
        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Archive type not recognized");
        }
        try {
            String containerName = archiveName + "." + archiveType.getExtension();
            File containerFile = new File(baseDir,containerName);
            //File containerFile = FileUtil.getTempFile("archive", "." + archiveType.getExtension());
            ArchiveBuilderBase archiveBuilder
                    = ArchiveBuilderBase.getArchiveBuilderBase(archiveDir, containerFile, logger, archiveType);
            archiveBuilder.buildArchive(includeBase);
            FileContent archiveContent = setFileContent(containerFile);
            return archiveContent;

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        } finally {
             try {
             } catch (Exception ex) { }
        }
    }

    public void buildArchive(
            boolean includeBase,
            OutputStream outputStream,
            String archiveTypeS
            )
        throws TException
    {
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilderBase.ArchiveType archiveType = null;
        try {
            archiveType
                    = ArchiveBuilderBase.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }
        
        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Archive type not recognized");
        }
        try {
            String containerName = archiveName + "." + archiveType.getExtension();
            ArchiveBuilderBase archiveBuilder
                    = ArchiveBuilderBase.getArchiveBuilderBase(
                        archiveDir, outputStream, logger, archiveType);
            
            archiveBuilder.buildArchive(includeBase);

        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        } finally {
             try {
             } catch (Exception ex) { }
        }
    }

    
    
    public void deleteArchiveDir()
    {
        try {
            if (archiveDir == null) {
                return;
            }
            FileUtil.deleteDir(archiveDir);

        } catch (Exception ex) {
            System.out.println("archiveDir not deleted:" + ex);
        }
        
    }
    
    /**
     * Set file state information from input file including fixity info
     * @param file get state of this file
     * @return File and FileState info
     * @throws TException process exception
     */
    protected FileContent setFileContent(File file)
        throws TException
    {
        FileComponent fileComponent = new FileComponent();
        FixityTests fixityTests = new FixityTests(file, "SHA-256", logger);
        fileComponent.addMessageDigest(fixityTests.getChecksum(), fixityTests.getChecksumType().toString());
        fileComponent.setIdentifier(file.getName());
        fileComponent.setSize(file.length());
        fileComponent.setCreated();
        tika.setTika(fileComponent, file, file.getName());
        FileContent fileContent = FileContent.getFileContent(fileComponent, null, file);
        return fileContent;
    }
}
