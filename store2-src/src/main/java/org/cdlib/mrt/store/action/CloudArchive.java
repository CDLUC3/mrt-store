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
package org.cdlib.mrt.store.action;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManInfo;

import org.cdlib.mrt.store.action.ArchiveComponent;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.action.CloudActionAbs;
import org.cdlib.mrt.cloud.utility.NormVersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowAdd;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.store.KeyFileInf;
import static org.cdlib.mrt.store.action.ArchiveComponentList.MESSAGE;
import org.cdlib.mrt.utility.ArchiveBuilderBase;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.PairtreeUtil;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * This object imports the formatTypes.xml and builds a local table of supported format types.
 * Note, that the ObjectFormat is being deprecated and replaced by a single format id (fmtid).
 * This change is happening because formatName is strictly a description and has no functional
 * use. The scienceMetadata flag is being dropped because the ORE Resource Map is more flexible
 * and allows for a broader set of data type.
 * 
 * @author dloy
 */
public class CloudArchive
        extends CloudActionAbs
{
    private static final String NAME = "CloudArchive";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    
    protected File workBase = null;
    protected File copyBase = null;
    protected ArrayList<ArchiveComponent> archiveList = new ArrayList<>();
    protected Tika tika = null;
    protected ArchiveBuilderBase.ArchiveType archiveType = null;
    protected String pairtreeName = null;
    protected boolean deleteFileAfterCopy = false;
    protected long addListMs = 0;
    protected long buildMs = 0;

    public CloudArchive(
            CloudStoreInf s3service, 
            String bucket, 
            Identifier objectID,
            File workBase, 
            String archiveTypeS,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        map =  getVersionMap(bucket, objectID);
        this.workBase = workBase;
        validate(archiveTypeS);
        this.archiveType = getArchiveType(archiveTypeS);
        tika = Tika.getTika(logger);
    }

    private void validate(String archiveTypeS)
        throws TException
    {
        try {
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger is null");
            }
            if (workBase == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "workBase is null");
            }
            if (!workBase.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "workBase does not exist:" + workBase.getCanonicalPath());
            }
            if (StringUtil.isAllBlank(archiveTypeS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "archiveType does not supplied");
            }
            pairtreeName = PairtreeUtil.getPairName(objectID.getValue());
            copyBase = new File(workBase, pairtreeName);
            if (copyBase.exists()) {
                FileUtil.deleteDir(copyBase);
            }
            copyBase.mkdir();
            
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID is null");
            }
            if (StringUtil.isEmpty(bucket)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "bucket name is null");
            }
            
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
        
    }

    public FileContent buildObject(String archiveName, boolean returnFullObject)
        throws TException
    {
        try {
            addListObject(returnFullObject);
            return buildArchive(true, copyBase, archiveName);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
            try {
                FileUtil.deleteDir(copyBase);
            } catch (Exception ex) { }
        }
        
    }

    public FileContent  buildVersion(int outVersion, String archiveName)
        throws TException
    {
        try {
            addListVersion(outVersion);
            return buildArchive(false, copyBase, archiveName);
            
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
            try {
                FileUtil.deleteDir(copyBase);
            } catch (Exception ex) { }
        }
        
    }

    public FileContent buildProducer(int outVersion, List<String> filterList, String archiveName)
        throws TException
    {
        try {
            ProducerComponent pc = new ProducerComponent(filterList, logger);
            addListProducer(outVersion, pc);
            return buildArchive(false, copyBase, archiveName);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
            try {
                FileUtil.deleteDir(copyBase);
            } catch (Exception ex) { }
        }
        
    }

    public void buildObject(OutputStream outputStream, boolean returnFullObject)
        throws TException
    {
        try {
            addListObject(returnFullObject);
            buildArchive(true, copyBase, outputStream);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
            try {
                FileUtil.deleteDir(copyBase);
            } catch (Exception ex) { }
        }
        
    }

    public void  buildVersion(int outVersion, OutputStream outputStream)
        throws TException
    {
        try {
            addListVersion(outVersion);
            buildArchive(false, copyBase, outputStream);
            
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
            try {
                FileUtil.deleteDir(copyBase);
            } catch (Exception ex) { }
        }
        
    }

    public void buildProducer(int outVersion, List<String> filterList, OutputStream outputStream)
        throws TException
    {
        try {
            ProducerComponent pc = new ProducerComponent(filterList, logger);
            addListProducer(outVersion, pc);
            buildArchive(false, copyBase, outputStream);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
            try {
                FileUtil.deleteDir(copyBase);
            } catch (Exception ex) { }
        }
        
    }
    
    protected void addListObject(boolean returnFullObject)
        throws TException
    {
        long startMs = System.currentTimeMillis();
        try {
            
            File newManifest = new File(copyBase, "manifest.xml");
            buildXMLManifest(map, newManifest);
            int current = map.getCurrent();
            if (current == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Object contains no versions");
            }
            for (int xver=1; xver <= current; xver++) {
                List<FileComponent> components= map.getVersionComponents(xver);
                File versionDir = new File(copyBase, "" + xver);
                versionDir.mkdirs();
                for (FileComponent component  : components) {
                    String key = component.getLocalID();
                    if (returnFullObject || key.contains("|" + xver + "|")) {
                        ArchiveComponent archiveComponent = ArchiveComponent.fromKey(key);
                        addFile(archiveComponent, versionDir);
                    }
                }
            }
            if (DEBUG) System.out.println("addListObject size=" + FileUtil.getDirectorySize(copyBase));
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
             addListMs = System.currentTimeMillis() - startMs;
        }
        
    }
    
    protected void addListVersion(int outVersion)
        throws TException
    {
        long startMs = System.currentTimeMillis();
        try {
            
            int current = map.getCurrent();
            if (current == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "Object contains no versions");
            }
            if (outVersion == 0) {
                outVersion = current;
            }
            if (outVersion > current) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "Requested version greater than current version:" + current);
            }
            
            List<FileComponent> components= map.getVersionComponents(outVersion);
            for (FileComponent component  : components) {
                String key = component.getLocalID();
                ArchiveComponent archiveComponent = ArchiveComponent.fromKey(key);
                addFile(archiveComponent, copyBase);
            }
            System.out.println("addListVersion size=" + FileUtil.getDirectorySize(copyBase));
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
             addListMs = System.currentTimeMillis() - startMs;
        }
        
    }
    
    protected void addListProducer(int outVersion, ProducerComponent pc)
        throws TException
    {
        long startMs = System.currentTimeMillis();
        try {
            
            int current = map.getCurrent();
            if (current == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "Object contains no versions");
            }
            if (outVersion == 0) {
                outVersion = current;
            }
            if (outVersion > current) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "Requested version greater than current version:" + current);
            }
            
            List<FileComponent> components= map.getVersionComponents(outVersion);
            for (FileComponent component  : components) {
                String key = component.getLocalID();
                ArchiveComponent archiveComponent = ArchiveComponent.fromKey(key);
                archiveComponent = pc.edit(archiveComponent);
                if (archiveComponent == null) continue;
                addFile(archiveComponent, copyBase);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
            
        } finally {
             addListMs = System.currentTimeMillis() - startMs;
        }
        
    }
    
    protected void addFile(ArchiveComponent component, File archiveDir)
        throws TException
    {
        try {
            File componentDir = archiveDir;
            if (component.filePath.length() > 0) {
                componentDir = new File(archiveDir, component.filePath);
                addDir(componentDir);
            }
            component.archiveFile = new File(componentDir, component.fileName);
            if (DEBUG) System.out.println("***addFile" 
                    + " - component.filePath:" + component.filePath
                    + " - componentDir:" + componentDir.getCanonicalPath()
                    + " - component.fileName:" + component.fileName
                    + " - component.archiveFile:" + component.archiveFile.getCanonicalPath()
            );
            CloudResponse response = new CloudResponse();
            s3service.getObject(bucket, component.key, component.archiveFile, response);
            if (response.getException() != null) {
                throw response.getException();
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
    
    
    /**
     * Get an archive file from a specific version
     * @param versionFile version directory file to return as full archive
     * @param archiveType type of archive (tar, targz, zip)
     * @return Archive file and state info of archive file
     * @throws TException process exception
     */
    protected FileContent buildArchive(
            boolean includeBase,
            File archiveDir,
            String archiveName)
        throws TException
    {
        
        try {
            String containerName = archiveName + "." + archiveType.getExtension();
            File containerFile = new File(workBase,containerName);
            //File containerFile = FileUtil.getTempFile("archive", "." + archiveType.getExtension());
            ArchiveBuilderBase archiveBuilder
                    = ArchiveBuilderBase.getArchiveBuilderBase(archiveDir, containerFile, logger, archiveType).setDeleteFileAfterCopy(deleteFileAfterCopy);
            archiveBuilder.buildArchive(includeBase);
            buildMs = archiveBuilder.getBuildTimeMs();
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
    

    protected void buildArchive(
            boolean includeBase,
            File archiveDir,
            OutputStream outputStream)
        throws TException
    {
        
        try {
            ArchiveBuilderBase archiveBuilder
                    = ArchiveBuilderBase.getArchiveBuilderBase(
                        archiveDir, outputStream, logger, archiveType).setDeleteFileAfterCopy(deleteFileAfterCopy);
            
            archiveBuilder.buildArchive(includeBase);
            buildMs = archiveBuilder.getBuildTimeMs();
            
        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE (
                    "Unable to find version.", ex);
        } finally {
             try {
             } catch (Exception ex) { }
        }
    }
    
    protected ArchiveBuilderBase.ArchiveType getArchiveType(String archiveTypeS)
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
        return archiveType;
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

    public String getPairtreeName() {
        return pairtreeName;
    }    
    
    public static void main_version(String[] args) throws TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        
        Properties xmlProp = new Properties();
        try {
            Identifier objectID = new Identifier("ark:/b5072/fk24t6n929");
            NodeService nodeService = NodeService.getNodeService("nodes-dev", 5001, logger);
            CloudStoreInf service = nodeService.getCloudService();
            String bucket = nodeService.getBucket();
            String baseDirS = "/apps/replic/test/github/170919-store/base";
            File baseDir = new File(baseDirS);
            CloudArchive cloudArchive = new CloudArchive(service, bucket, objectID, baseDir,"zip",  logger);
            FileContent fileContent = cloudArchive.buildVersion(3, "version");
            //cloudArchive.buildVersion(3);
            //cloudArchive.buildProducer(3, null);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
        
    }
    public static void main_object(String[] args) throws TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        
        Properties xmlProp = new Properties();
        try {
            Identifier objectID = new Identifier("ark:/b5072/fk24t6n929");
            NodeService nodeService = NodeService.getNodeService("nodes-dev", 5001, logger);
            CloudStoreInf service = nodeService.getCloudService();
            String bucket = nodeService.getBucket();
            String baseDirS = "/apps/replic/test/github/170919-store/base";
            File baseDir = new File(baseDirS);
            CloudArchive cloudArchive = new CloudArchive(service, bucket, objectID, baseDir,"zip",  logger);
            FileContent fileContent = cloudArchive.buildObject("mytest", false);
            //cloudArchive.buildVersion(3);
            //cloudArchive.buildProducer(3, null);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
        
    }
    public static void main_producer(String[] args) throws TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        
        Properties xmlProp = new Properties();
        try {
            Identifier objectID = new Identifier("ark:/b5072/fk24t6n929");
            NodeService nodeService = NodeService.getNodeService("nodes-dev", 5001, logger);
            CloudStoreInf service = nodeService.getCloudService();
            String bucket = nodeService.getBucket();
            String baseDirS = "/apps/replic/test/github/170919-store/base";
            File baseDir = new File(baseDirS);
            CloudArchive cloudArchive = new CloudArchive(service, bucket, objectID, baseDir,"zip",  logger);
            FileContent fileContent = cloudArchive.buildProducer(3, null, "producer");
            //cloudArchive.buildVersion(3);
            //cloudArchive.buildProducer(3, null);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
        
    }
    
    public static void main(String[] args) throws TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        
        Properties xmlProp = new Properties();
        try {
            Identifier objectID = new Identifier("ark:/b5072/fk24t6n929");
            NodeService nodeService = NodeService.getNodeService("nodes-dev", 5001, logger);
            CloudStoreInf service = nodeService.getCloudService();
            String bucket = nodeService.getBucket();
            String baseDirS = "/apps/replic/test/github/170919-store/base";
            File baseDir = new File(baseDirS);
            CloudArchive cloudArchive = new CloudArchive(service, bucket, objectID, baseDir,"zip",  logger);
            
            String objzipS = "/apps/replic/test/github/170919-store/base/outobjfull.zip";
            File objzip = new File(objzipS);
            FileOutputStream objstream = new FileOutputStream(objzip);
            cloudArchive.buildObject(objstream, true);
            //FileContent fileContent = cloudArchive.buildObject("mytest");
            //cloudArchive.buildVersion(3);
            //cloudArchive.buildProducer(3, null);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
        
    }

    public boolean isDeleteFileAfterCopy() {
        return deleteFileAfterCopy;
    }

    public CloudArchive setDeleteFileAfterCopy(boolean deleteFileAfterCopy) {
        this.deleteFileAfterCopy = deleteFileAfterCopy;
        return this;
    }

    public long getBuildMs() {
        return buildMs;
    }

    public long getAddListMs() {
        return addListMs;
    }
}
