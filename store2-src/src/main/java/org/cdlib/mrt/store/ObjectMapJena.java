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

import com.hp.hpl.jena.rdf.model.*;

import java.io.ByteArrayOutputStream;
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
public class ObjectMapJena
{
    protected static final String NAME = "ObjectMapJena";
    protected static final String MESSAGE = NAME + ":";
    protected final static String NL = System.getProperty("line.separator");

    protected LoggerInf logger = null;

    public static ObjectMapJena getObjectMapJena(
            LoggerInf logger)
        throws TException
    {
        return new ObjectMapJena(logger);
    }

    protected ObjectMapJena(
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
            Identifier objectID,
            String jenaFormat)
        throws TException
    {
        try {
            //log(MESSAGE + "buildMap entered");
            int versionID = getNextVersionID(storeURLS, nodeID, objectID);

            //System.out.println(MESSAGE + "!!!!" + "buildMap before getFileNames");
            Vector <String> fileNames = getFileNames(extractDirectory);

            //System.out.println(MESSAGE + "!!!!" + "buildMap before getAggregates");
            String [] dispAggregates = getAggregates(
                    fileNames,
                    resourceURLS,
                    nodeID,
                    objectID,
                    versionID);
            
            String objectResource = getObjectResource(
                    resourceURLS,
                    nodeID,
                    objectID);
            Model model = buildModel(objectResource, dispAggregates);
            String map = formatModel(model, jenaFormat);
            //!!!String [] aggregates)

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



    public String formatModel(Model model, String format)
        throws TException
    {
        String [] formats = {
            "RDF/XML",
            "RDF/XML-ABBREV",
            "N-TRIPLE",
            "TURTLE",
            "TTL",
            "N3"};


         try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
            model.write(baos, format);
            baos.close();
            byte[] bytes = baos.toByteArray();
            return new String(bytes, "utf-8");

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
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
        String objectState = null;
        try {
            StoreClient storeClient = StoreClient.getStoreClient(storeURLS, logger);
            try {
                objectState = storeClient.getObjectState ("xml", nodeID, objectID);
            } catch (Exception ex) {
                System.out.println("Object not found:" + storeURLS);
                return 1;
            }
            String numVersionsKey = "<obj:numVersions>";
            int pos = objectState.indexOf(numVersionsKey);
            if (pos < 0) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "getNextVersionID <numVersions> not found:"
                        + " - storeURLS=" + storeURLS
                        + " - objectState=" + objectState
                        );
            }
            int pos2 = objectState.indexOf("/obj:numVersions>", pos + numVersionsKey.length());
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
            fileNames.add("system/mrt-object-map.ttl");
            fileNames.add("system/mrt-splash.txt");
            FileUtil.getDirectoryFiles(extractDirectory, files);
            if (files.size() == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "No items found for:"
                        + extractDirectory.getCanonicalPath());
            }
            for (int i=0; i < files.size(); i++) {
                File file = files.get(i);
                String fileName = getFileName(extractDirectory, file);

                if (fileName.startsWith("mrt-")) continue;
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

    public String [] getAggregates(
            Vector<String> fileNames,
            String fileURLS,
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException
    {

        try {
            if (StringUtil.isEmpty(fileURLS)) {
                String msg = MESSAGE
                    + "getPOSTManifest - base URL not provided";
                throw new TException.INVALID_OR_MISSING_PARM( msg);
            }

            String [] arr = new String[fileNames.size()];
            for (int i=0; i < fileNames.size(); i++) {
                String fileName = fileNames.get(i);
                String fileURL = fileURLS
                        + "/content/" + nodeID
                        + "/" + URLEncoder.encode(objectID.getValue(), "utf-8")
                        + "/" + versionID
                        + "/" + URLEncoder.encode(fileName, "utf-8");
                URL testURL = new URL(fileURL);
                arr[i] = testURL.toString();
            }
            return arr;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }

    public Model buildModel(
            String objectResource,
            String [] aggregates)
    {
        String ore = "http://www.openarchives.org/ore/terms/";
        String mrt = "http://uc3.cdlib.org/ontology/mrt/mom#";
        String nie = "http://www.semanticdesktop.org/ontologies/2007/01/19/nie/";
        String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        String rdfs = "http://www.w3.org/2001/01/rdf-schema#";

        Model model = ModelFactory.createDefaultModel();
        model.setNsPrefix("ore", "http://www.openarchives.org/ore/terms/");
        model.setNsPrefix("nie", "http://www.semanticdesktop.org/ontologies/2007/01/19/nie/");
        Property oreAggregates = ResourceFactory.createProperty(ore + "aggregates");
        Property nieMimeType = ResourceFactory.createProperty(nie + "mimeType");
        Property nieIdentifier = ResourceFactory.createProperty(nie + "identifier");

        Resource object = model.createResource(objectResource);
        Resource [] objects = new Resource[aggregates.length];
        for (int i=0; i<aggregates.length; i++) {
            String aggregateS = aggregates[i];
            Resource aggregate = model.createResource(aggregateS);
            objects[i] = aggregate;
            model.add( object, oreAggregates, aggregate);
        }

        model.add(object, nieIdentifier, "ucsd:10326");
        model.add( objects[0], nieMimeType, "text/turtle" );
        model.add( objects[1], nieMimeType, "text/x-anvl" );
        return model;
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
        try {
                String resourceURL = resourceURLS
                        + "/object/" + nodeID
                        + "/" + URLEncoder.encode(objectID.getValue(), "utf-8")
                        + "/" + versionID;
                URL testURL = new URL(resourceURL);
            return testURL.toString();

        } catch(Exception ex) {
            String err = MESSAGE + "Could not complete version file output - Exception:" + ex;
            logger.logError(err ,  LoggerInf.LogLevel.UPDATE_EXCEPTION);
            logger.logError(StringUtil.stackTrace(ex),  LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( err);
        }
    }


    /**
     * Get this Object Resource
     * @param resourceURLS base URL
     * @param nodeID node identifier for resource
     * @param objectID object identifier for resource
     * @return String form URL for object resource
     * @throws TException process exception
     */
    public String getObjectResource(
            String resourceURLS,
            int nodeID,
            Identifier objectID)
        throws TException
    {
        try {
                String resourceURL = resourceURLS
                        + "/object/" + nodeID 
                        + "/" + URLEncoder.encode(objectID.getValue(), "utf-8");
                URL testURL = new URL(resourceURL);
            return testURL.toString();

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
