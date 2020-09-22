/*
Copyright (c) 2005-2010, Regents of the University of California
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
package org.cdlib.mrt.store;

import java.io.File;
import java.net.URL;

import org.cdlib.mrt.utility.URLEncoder;
import java.util.Vector;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.store.tools.StoreClient;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * State information for Object
 * @author dloy
 */
public class ObjectMap
{
    protected String NAME = "ObjectMap";
    protected String MESSAGE = NAME + ":";
    protected final static String NL = System.getProperty("line.separator");

    protected LoggerInf logger = null;

    public static ObjectMap getObjectMap(
            LoggerInf logger)
        throws TException
    {
        return new ObjectMap(logger);
    }

    protected ObjectMap(
            LoggerInf logger)
        throws TException
    {
        this.logger = logger;
    }


    public String buildMap(
            String storeURLS,
            String resourceURLS,
            File extractDirectory,
            int nodeID,
            Identifier objectID)
        throws TException
    {
        try {
            //log(MESSAGE + "buildMap entered");
            StringBuffer buf = new StringBuffer();
            int versionID = getNextVersionID(storeURLS, nodeID, objectID);
            String header =
                  "# Minimal Merritt object version resource map" + NL
                + "@prefix ore: <http://www.openarchives.org/ore/terms/>." + NL
                + "@prefix mrt:  <http://uc3.cdlib.org/ontology/mrt/mom#>." + NL
                + "@prefix nie:  <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/>." + NL
                + "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>." + NL
                + "@prefix rdfs: <http://www.w3.org/2001/01/rdf-schema#>." + NL;

            //System.out.println(MESSAGE + "!!!!" + "buildMap before header");
            buf.append(header);
            //System.out.println(MESSAGE + "!!!!" + "buildMap before getFileNames");
            Vector <String> fileNames = getFileNames(extractDirectory);
            //System.out.println(MESSAGE + "!!!!" + "buildMap before getStoreResource");
            String dispResource = getStoreResource(
                    resourceURLS,
                    nodeID,
                    objectID,
                    versionID);
            buf.append(dispResource);
            buf.append(" nie:identifier \"" + objectID.getValue() + "\";" + NL);
            buf.append(" ore:aggregates" + NL);

            //System.out.println(MESSAGE + "!!!!" + "buildMap before getAggregates");
            String dispAggregates = getAggregates(
                    fileNames,
                    resourceURLS,
                    nodeID,
                    objectID,
                    versionID);
            buf.append(dispAggregates);

            //System.out.println(MESSAGE + "!!!!" + "buildMap before getComponents");
            String dispComponents = getComponents(
                    fileNames,
                    resourceURLS,
                    nodeID,
                    objectID,
                    versionID);
            buf.append(dispComponents);

            String map = buf.toString();
            logger.logMessage("map=" + map, 10);
            System.out.println(MESSAGE + "map=" + map);
            return map;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            System.out.println(err);
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    protected int getNextVersionID(
            String storeURLS,
            int nodeID,
            Identifier objectID)
        throws TException
    {
        String response = null;
        try {
            StoreClient storeClient = StoreClient.getStoreClient(storeURLS, logger);

            String objectState = storeClient.getObjectState ("xml", nodeID, objectID);
            String numVersionsKey = "<numVersions>";
            if (objectState.contains("REQUESTED_ITEM_NOT_FOUND")) return 1;
            int pos = objectState.indexOf(numVersionsKey);
            if (pos < 0) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "getNextVersionID <numVersions> not found");
            }
            int pos2 = objectState.indexOf("/numVersions>", pos + numVersionsKey.length());
            String versionS = objectState.substring(pos + numVersionsKey.length(), pos2-1);
            //log("!!!!####pos=" + pos + " - pos2=" + pos2 + " - versionsS=" +  versionS);
            if (StringUtil.isEmpty(versionS)) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "<numVersions> empty");
            }
            int nextID = Integer.parseInt(versionS) + 1;
            return nextID;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    protected Vector<String> getFileNames(File extractDirectory)
        throws TException
    {
       try {
            Vector<File> files = new Vector<File>(1000);
            Vector<String> fileNames = new Vector<String>(1000);
            FileUtil.getDirectoryFiles(extractDirectory, files);
            if (files.size() == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "No items found for:"
                        + extractDirectory.getCanonicalPath());
            }
            for (int i=0; i < files.size(); i++) {
                File file = files.get(i);
                String fileName = getFileName(extractDirectory, file);

                if (fileName.startsWith("mrt-")) fileName = "system/" + fileName;
                else fileName = "producer/" + fileName;
                fileNames.add(fileName);
            }
            return fileNames;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not getFiles - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    public String getAggregates(
            Vector<String> fileNames,
            String fileURLS,
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {
        StringBuffer buf = new StringBuffer();

        try {
            if (StringUtil.isEmpty(fileURLS)) {
                String msg = MESSAGE
                    + "getPOSTManifest - base URL not provided";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }

            for (int i=0; i < fileNames.size(); i++) {
                String fileName = fileNames.get(i);
                String fileURL = fileURLS
                        + "/content/" + nodeID
                        + "/" + URLEncoder.encode(objectID.getValue(), "utf-8")
                        + "/" + versionID
                        + "/" + URLEncoder.encode(fileName, "utf-8");
                URL testURL = new URL(fileURL);
                buf.append("    <" + testURL + ">");
                if (i == (fileNames.size() - 1)) buf.append("." + NL);
                else buf.append("," + NL);
            }
            return buf.toString();

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    public String getComponents(
            Vector<String> fileNames,
            String fileURLS,
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {
        StringBuffer buf = new StringBuffer();
        try {
            String fileName = "system/mrt-object-map.ttl";
            String fileURL = fileURLS
                        + "/version/" + nodeID
                        + "/" + URLEncoder.encode(objectID.getValue(), "utf-8")
                        + "/" + versionID
                        + "/" + URLEncoder.encode(fileName, "utf-8");
            String retURL = "<" + fileURL + ">" + NL
                    //+ "mrt:MIME-type" + NL
                    + "  nie:mimeType" + NL
                    + "    \"text/turtle\"." + NL;
            buf.append(retURL);


            fileName = "system/mrt-splash.txt";
            fileURL = fileURLS
                        + "/version/" + nodeID
                        + "/" + URLEncoder.encode(objectID.getValue(), "utf-8")
                        + "/" + versionID
                        + "/" + URLEncoder.encode(fileName, "utf-8");
            retURL = "<" + fileURL + ">" + NL
                    //+ "mrt:MIME-type" + NL
                    + "  nie:mimeType" + NL
                    + "    \"x/anvl\"." + NL;
            buf.append(retURL);
            return buf.toString();

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
    public String getStoreResource(
            String resourceURLS,
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {
        StringBuffer buf = new StringBuffer();
        try {
                String resourceURL = resourceURLS
                        + "/object/" + nodeID 
                        + "/" + URLEncoder.encode(objectID.getValue(), "utf-8")
                        + "/" + versionID;
                URL testURL = new URL(resourceURL);
                buf.append("<" + testURL + ">" + NL);
            return buf.toString();

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    protected String getFileName(File sourceDir, File file)
        throws Exception
    {
            String fileName = file.getCanonicalPath();
            String sourceName = sourceDir.getCanonicalPath();
            fileName = fileName.substring(sourceName.length() + 1);
            fileName = fileName.replace('\\', '/');
            return fileName;
    }


    protected void log(String msg)
    {
        System.out.println(msg);
    }
}
