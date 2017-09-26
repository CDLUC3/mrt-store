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
package org.cdlib.mrt.cloud.action;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.ManifestXML;
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
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
/**
 * Abstract for performing a fixity test
 * @author dloy
 */
public class CloudActionAbs
{

    protected static final String NAME = "CloudActionAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String  STATUS_PROCESSING = "processing";
    private static final boolean DEBUG = false;

    
    protected CloudStoreInf s3service = null;
    protected String bucket = null;
    protected Identifier objectID = null;
    protected VersionMap map = null;
    protected LoggerInf logger = null;
    protected Exception exception = null;
    

    public CloudActionAbs(CloudStoreInf s3service, String bucket, Identifier objectID, LoggerInf logger)
    {
        this.s3service = s3service;
        this.bucket = bucket;
        this.logger = logger;
        this.objectID = objectID;
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
    
    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    protected VersionMap getVersionMapAdd(String bucket, Identifier objectID)
            throws TException
    {
        try {
            InputStream manifestXMLIn = s3service.getManifest(bucket, objectID);
            if (manifestXMLIn == null) {
                return new VersionMap(objectID, logger);
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    public VersionMap getVersionMap()
    {
        return map;
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

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
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
            int current = map.getCurrent();
            if (current == 0) return 0;
            int total = 0;
            for (int iv = 1; iv <= current; iv++) {
                ManInfo info = map.getVersionInfo(iv);
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
    
    protected void validateComponent(int versionID, FileComponent file)
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
    
}

