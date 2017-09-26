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
package org.cdlib.mrt.cloud.utility;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.io.InputStream;
import java.util.List;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
//import org.cdlib.mrt.s3.sdsc.SDSCCloud;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.core.Tika;
/**
 * Normalize VersionMap
 * - add mimeType
 * @author dloy
 */
public class NormVersionMap
{

    protected static final String NAME = "NormVersionMap";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;

    
    protected CloudStoreInf s3service = null;
    protected LoggerInf logger = null;
    protected Exception exception = null;
    protected Tika tika = null;


    public NormVersionMap(
            CloudStoreInf s3service, 
            LoggerInf logger)
        throws TException
    {
        this.s3service = s3service;
        this.logger = logger;
        tika = new Tika(logger);
    }
    
    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    public VersionMap getVersionMap(String bucket, Identifier objectID)
            throws TException
    {
        try {
            InputStream manifestXMLIn = s3service.getManifest(bucket, objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch(Exception ex) {
            throw throwEx(ex);
        }
    }
    
    
    /**
     * norm the content for this operation
     * @param map cloud version map
     * @param versionID normalize this version
     * @return current version ComponentContent
     * @throws TException 
     */
    public ComponentContent setVersion (
            VersionMap map,
            String bucket,
            Identifier objectID, 
            int versionID)
        throws TException
    {   
        
        HashMap<String,String> mimeList = new HashMap();
        try {
            int current = map.getCurrent();
            if (DEBUG) System.out.println("setVersion"
                    + " - current=" + current
                    + " - versionID=" + versionID
                    );
            if (current < 1) return null;
            if (versionID > current) return null;
            ComponentContent versionContent  = map.getVersionContent(versionID);
            List<FileComponent> inComponents = versionContent.getFileComponents();
            for (FileComponent component: inComponents) {
                
                String fileID = component.getIdentifier();
                String cloudKey = component.getLocalID();
                String mimeType = component.getMimeType();
                if (StringUtil.isNotEmpty(mimeType)) {
                    mimeList.put(cloudKey, mimeType);
                    continue;
                }
                mimeType = mimeList.get(cloudKey);
                if (StringUtil.isNotEmpty(mimeType)) {
                    component.setMimeType(mimeType);
                    log("***found in mimeList:" + cloudKey);
                    continue;
                }
                CloudResponse response = new CloudResponse(bucket, objectID, versionID, fileID);
                InputStream componentStream = s3service.getObject(bucket, cloudKey, response);
                mimeType = tika.getMimeType(componentStream, fileID);
                component.setMimeType(mimeType);
                mimeList.put(cloudKey, mimeType);
                try {
                    componentStream.close();
                } catch (Exception ex) { }
            }
            return versionContent;
        
        }  catch(Exception ex) {
            throw throwEx(ex);
        } 
    } 
    public VersionMap normalize(
            String bucket,
            Identifier objectID)
        throws TException
    {   
        VersionMap map = null;
        try {
            map = getVersionMap(bucket, objectID);
            return normalize(map, bucket, objectID);
        
        } catch(Exception ex) {
            throw throwEx(ex);
        }
    }
    
    /**
     * norm the content for this operation
     * @param map cloud version map
     * @param versionID normalize this version
     * @return current version ComponentContent
     * @throws TException 
     */
    public VersionMap normalize(
            VersionMap map,
            String bucket,
            Identifier objectID)
        throws TException
    {   
        try {
            String inMap = versionMap2String(map);
            log("In Map=\n" + inMap);
            int current = map.getCurrent();
            log("CURRENT=" + current);
            if (current < 1) return null;
            for (int versionID=1; versionID <= current; versionID++) {
                ComponentContent components = setVersion (map, bucket, objectID, versionID);
                if (components == null) throw new TException.INVALID_CONFIGURATION("bad current=" + current);
                log("processed version:" + versionID);
                
            }
            return map;
        
        } catch(Exception ex) {
            throw throwEx(ex);
        }
    }

    
    public void log(String msg)
    {
        if (DEBUG) System.out.println(msg);
        logger.logMessage(MESSAGE + msg, 10);
    }
    
    protected void log(String msg, int lvl)
    {
        if (DEBUG) System.out.println(msg);
        logger.logMessage(msg, lvl, true);
    }

    public TException throwEx(Exception ex)
    {
        if (exception != null) {
            if (exception instanceof TException) {
                return (TException) exception;
            } else {
                logger.logError("Exception:" + exception.toString(), 2);
                logger.logError("Trace:" + StringUtil.stackTrace(exception), 10);
                return new TException(exception);
            }
        } return new TException.INVALID_CONFIGURATION("requires exception as argument");
    }
    
    
    public static String versionMap2String(VersionMap map)
        throws TException
    {
        
        try {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
            ManifestStr.buildManifest(map, outStream);
            String outS = outStream.toString("utf-8");
            return outS;
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    /*
    public static void main(String[] args) throws TException {
        
        ManInfo test = new ManInfo();
        InputStream propStream =  test.getClass().getClassLoader().
                getResourceAsStream("resources/SDSC-S3.properties");
        if (propStream == null) {
            System.out.println("Unable to find resource");
            return;
        }
        Properties xmlProp = new Properties();
        try {
            xmlProp.load(propStream);
            LoggerInf logLocal = new TFileLogger(NAME, 50, 50);
            CloudStoreInf s3service = SDSCCloud.getSDSC(xmlProp, logLocal);
            String bucketName = "uc3a.bucket";
            Identifier objectID = new Identifier("ark:/13030/m52b8w1p");
            NormVersionMap normVersionMap = new NormVersionMap(s3service, logLocal);
            VersionMap map = normVersionMap.normalize(bucketName, objectID);
            ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
            ManifestStr.buildManifest(map, outStream);
            String outS = outStream.toString("utf-8");
            if (DEBUG) System.out.println("****Manifest.XML=\n" + outS);
            
            
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
    */
}

