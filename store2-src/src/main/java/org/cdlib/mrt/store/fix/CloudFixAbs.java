/******************************************************************************
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
*******************************************************************************/
package org.cdlib.mrt.store.fix;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;


import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
/**
 * Abstract for performing a fixity test
 * @author dloy
 */
public class CloudFixAbs
{

    protected static final String NAME = "CloudActionAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String  STATUS_PROCESSING = "processing";
    private static final boolean DEBUG = false;

    
    protected final Long nodeID;
    protected final NodeIO nodeIO;
    protected final Identifier objectID;
    protected final LoggerInf logger;
    protected CloudStoreInf s3service = null;
    protected String bucket = null;
    protected Exception exception = null;
    protected Integer versionID = null;
    protected Integer sizeChecksumBuffer = 16000000;
    protected VersionMap versionMap = null;
    protected File oldManFile = null;
    protected CloudResponse oldManResponse = null;
    
    protected ChangeComponent oldMapCC = null;
    protected LinkedHashMap<String, Integer> digestFileid = new LinkedHashMap<>();
    protected int objectCurrent = 0;
    protected static final Logger log4j = LogManager.getLogger();
    
    protected static final Logger logFix = LogManager.getLogger("FixLog");
    
    protected static final Logger logJson = LogManager.getLogger("FixJson");
    

    public CloudFixAbs(NodeIO nodeIO, Long nodeID, Identifier objectID, LoggerInf logger)
        throws TException
    {  
        if (nodeID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "nodeID- required");
        }
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "ark- required");
        }
        if (nodeIO == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "nodeIO - required");
        }
        this.nodeIO = nodeIO;
        this.nodeID = nodeID;
        this.logger = logger;
        this.objectID = objectID;
        setService();
    }
    
    private void setService()
        throws TException
    {
        try {
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(nodeID);
            if (accessNode == null) {
                
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                    MESSAGE + "nodeID not found:" + nodeID);
            }
            s3service = accessNode.service;
            bucket = accessNode.container;
            versionMap = getVersionMapStore(bucket, objectID);
            objectCurrent = versionMap.getCurrent();
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
        
    }
    
    protected void log(String msg, int lvl)
    {
        //System.out.println(msg);
        logger.logMessage(msg, lvl, true);
    }
    
    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    protected VersionMap getVersionMap(String bucket, Identifier objectID)
            throws TException
    {
        try {
            InputStream manifestXMLIn = s3service.getManifest(bucket, objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    protected VersionMap getVersionMapStore(String bucket, Identifier objectID)
            throws TException
    {
        try {
            String manifestKey = objectID.getValue() + "|manifest";
            oldManResponse = new CloudResponse(bucket, manifestKey);
            oldManFile = FileUtil.getTempFile("oldMap", ".txt");
            s3service.getObject(bucket, manifestKey, oldManFile, oldManResponse);
            if (oldManResponse.getException() != null) {
                throw new TException.GENERAL_EXCEPTION(oldManResponse.getException());
            }
            InputStream manifestXMLIn = new FileInputStream(oldManFile);
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    
    public VersionMap getVersionMap()
    {
        return versionMap;
    }
    
    /**
     * Get the current manifest.xml for this object
     * @param bucket cloud bucket
     * @param objectID object identifier
     * @param manifestFile output manifest file
     * @throws TException 
     */
    protected void getCloudManifestStream(String bucket, Identifier objectID, OutputStream outStream)
            throws TException
    {
        try {
            InputStream manifestXMLIn = s3service.getManifest(bucket, objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "no manifest found for this object");
            }
            FileUtil.stream2Stream(manifestXMLIn, outStream);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionContent", ex);
        }
    }
    
    /**
     * Get the current manifest.xml for this object
     * @param bucket cloud bucket
     * @param objectID object identifier
     * @param manifestFile output manifest file
     * @throws TException 
     */
    protected void getCloudManifest(String bucket, Identifier objectID, File manifestFile)
            throws TException
    {
        try {
            InputStream manifestXMLIn = s3service.getManifest(bucket, objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "no manifest found for this object");
            }
            FileUtil.stream2File(manifestXMLIn, manifestFile);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionContent", ex);
        }
    }
    
    /**
     * Get the current manifest.xml for this object
     * @param bucket cloud bucket
     * @param objectID object identifier
     * @throws TException 
     */
    public String getCloudManifestString(String bucket, Identifier objectID)
            throws TException
    {
        try {
            InputStream manifestXMLIn = s3service.getManifest(bucket, objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "no manifest found for this object");
            }
            return StringUtil.streamToString(manifestXMLIn, "utf-8");

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionContent", ex);
        }
    }
    
    /**
     * Build file containing XML form of manifest
     * @param map VersionMap containing content of manifest
     * @param newManifest if non-null, file to be used for output manifest - null, create temp
     * @return file containing output manifest
     * @throws TException 
     */
    public File buildXMLManifest (VersionMap map, File newManifest)
        throws TException
    {   
        try {
            if (newManifest == null) {
                newManifest = FileUtil.getTempFile("manifest", ".xml");
            }
            ManifestStr.buildManifest(map, newManifest);
            return newManifest;

        } catch(Exception ex) {
            throw new TException(ex);
        }
    }
    
    public String dumpVersionMap(VersionMap map)
        throws TException
    {
        File xmlMap = buildXMLManifest(map, null);
        return FileUtil.file2String(xmlMap);
    }
    
    /**
     * Delete the content for this operation
     * @param map cloud version map
     * @param versionContent file content for this version
     * @return current version
     * @throws TException 
     */
    public int deleteContent (VersionMap map, ComponentContent versionContent)
        throws TException
    {   
        try {
            int current = map.getCurrent();
            if (current < 1) return 0;
            List<FileComponent> inComponents = versionContent.getFileComponents();
            for (FileComponent component: inComponents) {
                map.setCloudComponent(component, true);
                boolean alphaNumeric = s3service.isAlphaNumericKey();
                String key = CloudUtil.getKey(objectID, current, component.getLocalID(), alphaNumeric);
                if (key.equals(component.getLocalID())) { // not a delta
                    CloudResponse response = s3service.deleteObject(bucket, component.getLocalID());
                }
            }
            
            map.deleteCurrent();
            File newManifest = FileUtil.getTempFile("manifest", ".xml");
            ManifestStr.buildManifest(map, newManifest);
            
            s3service.putManifest(bucket, objectID, newManifest);
            newManifest.delete();
            return map.getCurrent();

        } catch(Exception ex) {
            setException(ex);
            return 0;
        }
    }
    
    /**
     * Delete the content for this operation
     * @param map cloud version map
     * @return current version
     * @throws TException 
     */
    public int deleteCurrent (VersionMap map)
        throws TException
    {   
        try {
            int current = map.getCurrent();
            if (current < 1) return 0;
            ComponentContent versionContent  = map.getVersionContent(current);
            List<FileComponent> inComponents = versionContent.getFileComponents();
            for (FileComponent component: inComponents) {
                map.setCloudComponent(component, true);
                boolean alphaNumeric = s3service.isAlphaNumericKey();
                String key = CloudUtil.getKey(objectID, current, component.getIdentifier(), alphaNumeric);
                if (DEBUG) {
                    System.out.println(MESSAGE + "deleteCurrent:" + current + "\n"
                            + " - alphaNumeric=" + alphaNumeric + "\n"
                            + " - component.getLocalID()=" + component.getLocalID() + "\n"
                            + " - key=" + key + "\n"
                            );
                }
                if (key.equals(component.getLocalID())) { // not a delta
                    CloudResponse response = s3service.deleteObject(bucket, objectID, current,  component.getIdentifier());
                }
            }
            
            map.deleteCurrent();
            File newManifest = FileUtil.getTempFile("manifest", ".xml");
            ManifestStr.buildManifest(map, newManifest);
            
            s3service.putManifest(bucket, objectID, newManifest);
            newManifest.delete();
            return map.getCurrent();

        } catch(Exception ex) {
            setException(ex);
            return 0;
        }
    }
    
    /**
     * create TException and do appropriate logger
     * @param logger process logger
     * @param msg error message
     * @param ex encountered exception to convert
     * @return TException
     */
    public static TException makeGeneralTException(
            LoggerInf logger,
            String msg,
            Exception ex)
    {
        logger.logError(msg + ex,
                LoggerInf.LogLevel.UPDATE_EXCEPTION);
        logger.logError(msg + " - trace:"
                + StringUtil.stackTrace(ex),
                LoggerInf.LogLevel.DEBUG);
        return new TException.GENERAL_EXCEPTION(
                msg +  "Exception:" + ex);
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Identifier getObjectID() {
        return objectID;
    }
    
    public void log(String msg)
    {
        if (DEBUG) System.out.println(msg);
        logger.logMessage(MESSAGE + msg, 10);
    }
    
    public String formatXML(StateInf responseState)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getXMLFormatter(logger);
           return formatIt(xml, responseState);
    }
    
    public static String formatXML(StateInf responseState, LoggerInf logger)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getXMLFormatter(logger);
           return formatIt(xml, responseState);
    }

    public static String formatIt(
            FormatterInf formatter,
            StateInf responseState)
    {
        try {
           ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
           PrintStream  stream = new PrintStream(outStream, true, "utf-8");
           formatter.format(responseState, stream);
           stream.close();
           byte [] bytes = outStream.toByteArray();
           String retString = new String(bytes, "UTF-8");
           return retString;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return null;
        }
    }


    public void throwEx()
        throws TException
    {
        if (exception != null) {
            if (exception instanceof TException) {
                throw (TException) exception;
            } else {
                logger.logError("Exception:" + exception.toString(), 2);
                logger.logError("Trace:" + StringUtil.stackTrace(exception), 10);
                throw new TException(exception);
            }
        }
    }
    
    protected static void deleteWorkBase(File workBase)
    {
        try {
            if ((workBase != null) && workBase.exists()) {
                FileUtil.deleteDir(workBase);
            }
        } catch (Exception ex) { }
    }
    
    protected int doValidation()
        throws TException
    {
        try {
            int current = versionMap.getCurrent();
            if (current == 0) return 0;
            int total = 0;
            for (int iv = 1; iv <= current; iv++) {
                ManInfo info = versionMap.getVersionInfo(iv);
                int cnt = validateManInfo(info);
                total += cnt;
            }
            return total;
            
        } catch (TException tex) {
            throw tex;

        } catch(Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected int validateManInfo(ManInfo info)
        throws TException
    {
            ComponentContent content = info.components;
            List<FileComponent> list = content.getFileComponents();
            int cnt = 0;
            for (FileComponent file : list) {
                validateComponent(info.versionID, file);
                cnt++;
            }
            return cnt;
    }
    
    protected void validateComponentOriginal(int versionID, FileComponent file)
        throws TException
    {
        try {
            String key = file.getLocalID();
            if (DEBUG) System.out.println("***validateComponent entered"
                    + " - key=" + key
                    );
            MessageDigest digest = file.getMessageDigest();
            String checksumType = digest.getJavaAlgorithm();
            String checksum = digest.getValue();
            CloudResponse response = new CloudResponse(bucket, objectID, versionID, file.getIdentifier());
            response.setStorageKey(key);
            //InputStream fileStream = s3service.getObject(bucket, key, response);
            InputStream fileStream = s3service.getObject(bucket, objectID, versionID, file.getIdentifier(), response);
            FixityTests tests = new FixityTests(fileStream, checksumType, logger);
            
            FixityTests.FixityResult fixityResult 
                    = tests.validateSizeChecksum(checksum, checksumType, file.getSize());
            FixityTests.Result result= tests.getResult();
            String msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + file.getIdentifier()
                        + " - key=" + key
                        + " - checksumType=" + checksumType
                        + " - manifestChecksum=" + checksum
                        + " - manifestSize=" + file.getSize()
                        + " - returnChecksum=" + result.checksum
                        + " - returnSize=" + result.inputSize
                        + " - " + fixityResult.dump("ValidateComponent");
            if (!fixityResult.fileSizeMatch || !fixityResult.checksumMatch) {
                logger.logMessage("Component fixity FAILS:" + msg, 0, true);
                throw new TException.FIXITY_CHECK_FAILS("Component fixity FAILS " + msg);
                
            } else {
                    msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + file.getIdentifier()
                        + " - key=" + key;
                logger.logMessage("Component fixity OK:" + msg, 0, true);
                if (DEBUG) System.out.println("validateComponent OK:" + msg);
            }
            
        } catch (TException tex) {
            throw tex;

        } catch(Exception ex) {
            throw new TException(ex);
            
        }
    }
    
    public void validateComponent(int versionID, FileComponent file)
        throws TException
    {
        try {
            String key = file.getLocalID();
            if (DEBUG) System.out.println("***validateComponent entered"
                    + " - key=" + key
                    );
            MessageDigest digest = file.getMessageDigest();
            String checksumType = digest.getJavaAlgorithm();
            String checksum = digest.getValue();
            long fileSize = file.getSize();
            
            String [] types = new String[1];
            types[0] = checksumType;

            CloudChecksum cc = CloudChecksum.getChecksums(types, s3service, bucket, key, sizeChecksumBuffer);

            cc.process();
            CloudChecksum.CloudChecksumResult fixityResult
                    = cc.validateSizeChecksum(checksum, checksumType, fileSize, logger);
            String returnChecksum = cc.getChecksum(checksumType);
           
            String msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + file.getIdentifier()
                        + " - key=" + key
                        + " - checksumType=" + checksumType
                        + " - manifestChecksum=" + checksum
                        + " - manifestSize=" + file.getSize()
                        + " - returnChecksum=" + returnChecksum
                        + " - returnSize=" + cc.getInputSize()
                        + " - " + fixityResult.dump("ValidateComponent");
            if (!fixityResult.fileSizeMatch || !fixityResult.checksumMatch) {
                logger.logMessage("Component fixity FAILS:" + msg, 0, true);
                throw new TException.FIXITY_CHECK_FAILS("Component fixity FAILS " + msg);
                
            } else {
                    msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + file.getIdentifier()
                        + " - key=" + key;
                logger.logMessage("Component fixity OK:" + msg, 0, true);
                if (DEBUG) System.out.println("validateComponent OK:" + msg);
            }
            
        } catch (TException tex) {
            throw tex;

        } catch(Exception ex) {
            throw new TException(ex);
            
        }
    }
    
    public static  String removeQueryProp(String propName, String oldFileID)
        throws TException
    {
        int posQues = oldFileID.indexOf("?");
        if (posQues < 0) return oldFileID;
        String p1 = oldFileID.substring(0,posQues);
        String query = oldFileID.substring(posQues+1);
        String newQuery = "";
        String [] ps = query.split("&");
        for (String p : ps) {
            if (p.contains(propName+"=")) {
                continue;
            }
            if (newQuery.length() > 0) {
                newQuery = newQuery + "&";
            } else {
                newQuery = "?";
            }
            newQuery = newQuery + p;
        }
        String returnID = p1 + newQuery;
        return returnID;
    }
    
    protected void logFixMsg(Boolean run, String type, String bucket, String fromKey, String toKey, long length)
    {
        
        logFix.info("Run(" + run + ") "
                + " - type=" + type
                + " - bucket=" + bucket
                + " - length=" + length
                + " - fromKey=" + fromKey
                + " - toKey=" + toKey
        );
    }
    
    protected static LinkedHashMap<String, Integer> buildDigestFileid(VersionMap map)
        throws TException
    {
        try {
            LinkedHashMap<String, Integer>digestFileid = new LinkedHashMap<>();
            int current = map.getCurrent();
            for (int versionId=1; versionId<=current; versionId++) {
                List<FileComponent> components = map.getVersionComponents(versionId);
                for (FileComponent component : components) {
                    String dfkey = getDigestFileidKey(component);
                    Integer extractVersion = digestFileid.get(dfkey);
                    if (extractVersion == null) {
                        digestFileid.put(dfkey, versionId);
                    }
                    
                }
            }
            return digestFileid;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    protected int addDigestFileid(int versionId, FileComponent component)
        throws TException
    {
        try {
            
            String dfkey = getDigestFileidKey(component);
            Integer extractVersion = digestFileid.get(dfkey);
            if (extractVersion == null) {
                digestFileid.put(dfkey, versionId);
                return versionId;
            } else {
                return extractVersion;
            }
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    protected static String getDigestFileidKey(FileComponent component)
    {
        String fileId = component.getIdentifier();
        String sha256 = component.getMessageDigest().getValue();
        Long len = component.getSize();
        String dfkey = sha256 + "|" + len + "|" + fileId;
        return dfkey;
    }
    
    protected Integer getDigestFileidVersion(FileComponent component)
    {
        String dfkey = getDigestFileidKey(component);
        Integer extractVersion = digestFileid.get(dfkey);
        return extractVersion;
    }
    
    protected void dumpDigestFileid(String hdr)
    {
        System.out.println("\n\n*** dumpDigestFileid ***:" + hdr + "\n");
        Set<String> keys = digestFileid.keySet();
        for (String dfkey : keys) {
                Integer extractVersion = digestFileid.get(dfkey);
                System.out.println(dfkey + "==" + extractVersion);
            
        }
    }
}

