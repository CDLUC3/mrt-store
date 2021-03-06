/*
Copyright (c) 2005-2009, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

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
*********************************************************************/
package org.cdlib.mrt.store.tools;

import org.cdlib.mrt.store.test.*;
import java.io.File;
import java.io.InputStream;
import java.net.URL;

import org.cdlib.mrt.utility.URLEncoder;
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.ListProcessor;
import org.cdlib.mrt.utility.ListProcessorThreads;
import org.cdlib.mrt.utility.LoggerInf.LogLevel;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;

import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowAdd;
import org.cdlib.mrt.store.ObjectMapJena;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Run a Storage load test
 *
 * @author dloy
 */

public class StoreLoaderJena
        extends ListProcessorThreads
        implements ListProcessor
{

    public enum Tests {
        test1,test2
    }
    protected String NAME = "StoreLoaderJena";
    protected String MESSAGE = NAME + ":";
    public static final String LS =  System.getProperty("line.separator");
    public static final int DEFAULTDELTA = 25;

    protected File dataFile = null;
    protected File logDirectory = null;
    //protected String manifestURLS = null;
    protected String dataURLS = null;
    protected String storeURLS = null;
    protected String resourceURLS = null;
    protected ObjectMapJena objectMap = null;

    Vector<String> manifestURLs = new Vector<String>(10);
    Vector<Integer> addSizes = new Vector<Integer>(10);
    protected int delta = 0;
    protected int nodeID = -1;
    protected File statsDir = null;
    protected Tests runTest = Tests.test1;

    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        try
        {

            String propertyList[] = {
                "testresources/TestLocal.properties"};
            framework = new TFrame(propertyList, "StoreLoader");
            
            // Create an instance of this object
            StoreLoaderJena test = new StoreLoaderJena(framework);
            test.run();
        }
        catch(Exception e)
        {
            if (framework != null)
            {
                framework.getLogger().logError(
                    "Main: Encountered exception:" + e, 0);
                framework.getLogger().logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }
    
    public StoreLoaderJena(TFrame framework)
        throws TException
    {
        super(framework);
        initialize(framework);
    }
    
    private void initialize(TFrame fw)
        throws TException
    {

        try {
            Properties clientProperties = getClientProperties();
            dataURLS = clientProperties.getProperty("dataURL");
            if (StringUtil.isEmpty(dataURLS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "dataURL required");
            }
            String logDirectoryS = clientProperties.getProperty("logDirectory");
            if (StringUtil.isEmpty(logDirectoryS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logDirectory required");
            }
            logDirectory = new File(logDirectoryS);
            if (!logDirectory.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logDirectory does not exist:" + logDirectoryS);
            }
            String dataFileS = clientProperties.getProperty("dataFile");
            if (StringUtil.isEmpty(dataFileS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "dataFile required");
            }
            dataFile = new File(dataFileS);
            if (!dataFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "dataFile does not exist:" + dataFileS);
            }
            storeURLS = clientProperties.getProperty("storeURL");
            if (StringUtil.isEmpty(storeURLS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "storageURL required");
            }
            resourceURLS = clientProperties.getProperty("resourceURL");
            if (StringUtil.isEmpty(storeURLS)) {
                resourceURLS = storeURLS;
            }
            String nodeIDS = clientProperties.getProperty("nodeID");
            if (StringUtil.isEmpty(nodeIDS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeID required");
            }
            try {
                nodeID = Integer.parseInt(nodeIDS);
            } catch (Exception nex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeID invalid:" + nodeIDS);
            }
            objectMap = ObjectMapJena.getObjectMapJena(logger);
            System.out.println("Run test:" + runTest.toString());
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Unable to load db manager: Exception:" + ex);
        }
    }
        

    
    /**
     * Name of EnrichmentList
     * @return name to be applied for identifying process properties
     */
    @Override
    public String getName() 
    {
        return "StoreLoaderJena";
    }

    /**
     * <pre>
     * Process an incoming line which contains
     * 1) ark
     * 2) sequence number
     * Use the ark for issuing an addVersion to a storage service
     * </pre>
     * @param line line containing ark and sequence number
     * @param processProp runtime properties
     * @throws TException process exception
     */
    @Override
    public void process(
            String line,
            Properties processProp)
	throws TException
    {
        File manifest = null;
        String checksumType = processProp.getProperty("checksumType", "SHA-256");
        try {
            log("LINE:" + line);
            String[] items = line.split("\\s+");
            if ((items == null) || (items.length != 2)) {
                throw new TException.INVALID_DATA_FORMAT(
                        MESSAGE + "entry format invalid: line=" + line
                        + " - linecnt=" + items.length);
            }
            String objectIDS = items[1];
            String localID = items[0];
            Identifier.Namespace nameSpace = null;
            if (objectIDS.startsWith("ark:")) nameSpace = Identifier.Namespace.ARK;
            else nameSpace = Identifier.Namespace.Local;
            Identifier objectID = new Identifier(objectIDS, nameSpace);

            File localDataFile = new File(dataFile, localID);
            if (!localDataFile.exists()) {
                logger.logError("localDataFile missing:" + localDataFile.getAbsolutePath(), 0);
                return;
            }
            //System.out.println("!!!!: before objectMap buildMap");
            String objectMapS = objectMap.buildMap(
                    storeURLS,
                    resourceURLS,
                    localDataFile,
                    nodeID,
                    objectID,
                    "TURTLE");
            File resourceFile = new File(localDataFile, "mrt-object-map.ttl");
            FileUtil.string2File(resourceFile, objectMapS);
            manifest = getManifest(line, checksumType, objectID, localID);
            if (manifest == null) return;

            String response = addVersion(objectID, manifest);
            log("Response=" + response);
            String localDate = DateUtil.getCurrentDateString("yyyy-MM-dd'T'HH-mm-ss");
            File responseFile = new File(logDirectory, localID + "." + localDate + ".xml");
            FileUtil.string2File(responseFile, response);
            String manifestString = FileUtil.file2String(manifest);
            log("manifest=" + manifestString);


        }  catch(TException fex){
            m_framework.getLogger().logError(
                MESSAGE + " Exception: " + fex,
                3);
            m_status.bump(line + ".fail");
            throw fex;

        } catch(Exception ex){
            m_framework.getLogger().logError(
                MESSAGE + " Exception: " + ex,
                3);
            m_framework.getLogger().logError(
                MESSAGE + " StackTrace: " + StringUtil.stackTrace(ex), 10);
            m_status.bump(line + ".fail");
            throw new TException.GENERAL_EXCEPTION(ex);
            
        }  finally {
            try {
                if (manifest != null)
                    manifest.delete();
            } catch (Exception ex) {}
        }
    }

    public File getManifest(
            String line,
            String checksumType,
            Identifier objectID,
            String localID)
	throws TException
    {
        File postManifestFile = null;
        try {
            File localDataFile = new File(dataFile, localID);
            if (!localDataFile.exists()) {
                logger.logError("localDataFile missing:" + localDataFile.getAbsolutePath(), 0);
                return null;
            }
            String localDataURLS = dataURLS + "/" + localID;
            postManifestFile = FileUtil.getTempFile("tmp", "txt");
            ManifestInfo propInfo = getPostManifest(localDataURLS, checksumType, localDataFile, postManifestFile);
            return postManifestFile;

        }  catch(TException fex){
            m_framework.getLogger().logError(
                MESSAGE + " Exception: " + fex,
                3);
            m_status.bump(line + ".fail");
            throw fex;

        } catch(Exception ex){
            m_framework.getLogger().logError(
                MESSAGE + " Exception: " + ex,
                3);
            m_framework.getLogger().logError(
                MESSAGE + " StackTrace: " + StringUtil.stackTrace(ex), 10);
            m_status.bump(line + ".fail");
            throw new TException.GENERAL_EXCEPTION(ex);

        }
    }


    /**
     * Build a POST manifest
     * @param fileURLS base URL for deriving manifest fileURLs
     * @param sourceDir directory file containing files for manifest generation
     * @param postManifest output file to contain POST manifest
     * @return accumulated size of files referenced by manifest
     * @throws TException process exception
     */
    public ManifestInfo getPostManifest(
            String fileURLS,
            String checksumType,
            File sourceDir,
            File postManifest)
        throws TException
    {
        Vector<String> fileNames = new Vector<String>(100);
        ManifestInfo manifestInfo = new ManifestInfo();
        manifestInfo.fileNames = fileNames;
        manifestInfo.manifestFile = postManifest;

        ManifestRowAbs.ManifestType manifestType = ManifestRowAbs.ManifestType.add;
        try {
            if (StringUtil.isEmpty(fileURLS)) {
                String msg = MESSAGE
                    + "getPOSTManifest - base URL not provided";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }
            Vector<File> files = new Vector<File>(1000);
            FileUtil.getDirectoryFiles(sourceDir, files);
            if (files.size() == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "No items found for:"
                        + sourceDir.getCanonicalPath());
            }
            ManifestRowAdd rowOut
                    = (ManifestRowAdd)ManifestRowAbs.getManifestRow(manifestType, logger);
            Manifest manifestDflat = Manifest.getManifest(logger, manifestType);
            manifestDflat.openOutput(postManifest);

            long totSize = 0;
            long cnt = 0;
            for (File file : files) {
                FileComponent fileState =  addManifestRow(file, checksumType, fileURLS, sourceDir);
                fileNames.add(fileState.getIdentifier());
                rowOut.setFileComponent(fileState);
                log("getPostManifest-line:" + rowOut.getLine());
                manifestDflat.write(rowOut);
                totSize += fileState.getSize();
                cnt++;
            }
            manifestInfo.size = totSize;
            manifestInfo.cnt = cnt;
            manifestDflat.writeEOF();
            manifestDflat.closeOutput();
            System.out.println("***MANIFEST***" + FileUtil.file2String(postManifest));

            return manifestInfo;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }



    /**
     * Build a POST manifest
     * @param fileURLS base URL for deriving manifest fileURLs
     * @param sourceDir directory file containing files for manifest generation
     * @param postManifest output file to contain POST manifest
     * @return accumulated size of files referenced by manifest
     * @throws TException process exception
     */
    public FileComponent addManifestRow(
            File file,
            String checksumType,
            String fileURLS,
            File sourceDir)
        throws TException
    {

        try {
            FileContent fileContent = FileContent.getFileContent(file, checksumType, logger);
            FileComponent fileState = fileContent.getFileComponent();
            String fileName = file.getCanonicalPath();
            String sourceName = sourceDir.getCanonicalPath();
            fileName = fileName.substring(sourceName.length() + 1);
            fileName = fileName.replace('\\', '/');
            String manifestURLName = fileName;

            if (fileName.startsWith("mrt-")) fileName = "system/" + fileName;
            else fileName = "producer/" + fileName;

            log("fileName:" + fileName);
            log("sorsName:" + sourceName);
            fileState.setIdentifier(fileName);
            URL fileLink = null;
            try {
                fileLink = new URL(fileURLS + '/' + URLEncoder.encode(manifestURLName, "utf-8"));
                //fileLink = new URL(fileURLS + '/' + manifestURLName); // <-BAD FORM for testing ONLY
            } catch (Exception ex) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE
                        + "getPOSTManifest"
                        + " - passed URL format invalid: getFileURL=" + fileURLS
                        + " - Exception:" + ex);
            }
            fileState.setURL(fileLink);
            return fileState;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    protected String addVersion(Identifier objectID, File manifest)
        throws TException
    {
        String response = null;
        try {
            StoreClient storeClient = StoreClient.getStoreClient(storeURLS, logger);
            return storeClient.addVersion("xml", nodeID, objectID, manifest);

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    protected void log(String msg)
    {
        System.out.println(msg);
    }


    public class ManifestInfo {
        public long cnt = 0;
        public long size = 0;
        public File manifestFile = null;
        public Vector<String> fileNames = null;
    }
}
