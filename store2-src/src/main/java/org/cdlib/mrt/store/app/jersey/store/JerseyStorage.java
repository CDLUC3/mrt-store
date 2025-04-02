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
package org.cdlib.mrt.store.app.jersey.store;

import java.net.URI;
import java.net.URL;
import org.cdlib.mrt.store.app.jersey.*;
import org.cdlib.mrt.store.app.*;

import java.util.Properties;
import javax.servlet.ServletConfig;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.server.CloseableService;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.QueryParam;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.cloud.utility.CloudUtil;
import org.cdlib.mrt.log.utility.Log4j2Util;
import org.cdlib.mrt.store.StoreNodeManager;
import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.store.storage.StorageService;
import org.cdlib.mrt.store.storage.StorageServiceInf;
import org.cdlib.mrt.store.app.jersey.KeyNameHttpInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.NodeIOState;
import org.cdlib.mrt.s3.service.NodeIOStatus;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 * Thin Jersey layer for Storage servlet
 * @author  David Loy
 */
@Path ("/")
public class JerseyStorage 
        extends JerseyBase
        implements KeyNameHttpInf
{

    protected static final String NAME = "JerseyStorage";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT
            = FormatterInf.Format.xml;
    protected static final boolean DEBUG = false;
    protected static final String NL = System.getProperty("line.separator");



    @GET
    @Path("/state")
    public Response getServiceState(
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = storageService.getLogger();
            StateInf responseState = storageService.getServiceState();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    @GET
    @Path("/jsonstatus")
    public Response getJsonStatus(
            @DefaultValue("5") @QueryParam("timeout") String timeoutSecS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        
        try {
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            StorageConfig storageConfig = storageService.getStorageConfig();
            NodeIO nodeIO = storageConfig.getNodeIO();
            int timeoutSec = 5;
            try {
                timeoutSec = Integer.parseInt(timeoutSecS);;
            } catch (Exception ex) { 
                log4j.debug("Invalid value sent jsonstatus - process continues - timeoutSecS:" + timeoutSecS);
            }
            JSONObject state = NodeIOStatus.runStatus(nodeIO, timeoutSec);
            return Response 
                .status(200).entity(state.toString())
                    .build();
              
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    @POST
    @Path("/reset")
    public Response getResetState(
            @DefaultValue("-none-") @QueryParam("log4jlevel") String log4jlevel,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log4j.info("getResetState entered:"
                    + " - formatType=" + formatType
                    + " - log4jlevel=" + log4jlevel
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.resetStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = storageService.getLogger();
            StateInf responseState = storageService.getServiceState();
            if (!log4jlevel.equals("-none-")) {
                Log4j2Util.setRootLevel(log4jlevel);
            }
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
        
    @GET
    @Path("ping")
    public Response getCallPingState(
            @DefaultValue("") @QueryParam("SLEEP") String secondsS,
            @DefaultValue("false") @QueryParam("gc") String gcS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {

        if (!StringUtil.isAllBlank(secondsS)) {
            try {
                int seconds = 0;
                seconds = Integer.parseInt(secondsS);
                System.out.println("Ping sleep requested:" + seconds + "seconds");
                Thread.sleep(seconds*1000);
                
            } catch (Exception ex) {
                System.out.println("Sleep not performed Provided SLEEP seconds invalid:" 
                            + secondsS);
            }
        }
        return getPingState(gcS, formatType, cs, sc);
    }
        
    @GET
    @Path("state/{nodeid}")
    public Response getNodeState(
            @PathParam("nodeid") String nodeIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getNodeState(nodeID, formatType, cs, sc);
    }
        
    @GET
    @Path("node/{objectid}")
    public Response findObjectState(
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        
        Identifier objectID = new Identifier(objectIDS);
        return findObjectState(objectID, formatType, cs, sc);
    }

    @GET
    @Path("state/{nodeids}/{objectid}")
    public Response getObjectState(
            @PathParam("nodeids") String nodeIDsS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int [] nodeIDs = getNodeIDs(nodeIDsS);
        return getObjectState(nodeIDs, objectIDS, formatType, cs, sc);
    }

    @GET
    @Path("state/{nodeid}/{objectid}/{versionid}")
    public Response getVersionState(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getVersionStateProcess(nodeID, objectIDS, versionIDS, formatType, cs, sc);
    }

    @GET
    @Path("state/{nodeid}/{objectid}/{versionid}/{fileid}")
    public Response getFileState(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @PathParam("fileid") String fileID,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getFileState(nodeID, objectIDS, versionIDS, fileID, formatType, cs, sc);
    }
    
    @POST
    @Path("accesslock/{type}/{operation}")
    public Response callAccessLock(
            @PathParam("type") String type,
            @PathParam("operation") String operation,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        System.out.println("callAccessLock" 
                + " - type:" + type
                + " - operation:" + operation
        );
        JSONObject jsonStatus  = processAccessLock(type, operation, cs, sc);
        boolean OK = jsonStatus.getBoolean("OK");
        String jsonS = jsonStatus.toString();
        
        int status = 200;
        if (!OK) status = 500;
        return Response 
                    .status(status).entity(jsonS)
                    .build();
    }


    @GET
    @Path("fixity/{nodeid}/{objectid}/{versionid}/{fileid}")
    public Response getFileFixityState(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @PathParam("fileid") String fileID,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getFileFixityState(nodeID, objectIDS, versionIDS, fileID, formatType, cs, sc);
    }

    @GET
    @Path("fixity/{nodeid}/{objectid}")
    public Response getObjectFixityState(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getObjectFixityState(nodeID, objectIDS, formatType, cs, sc);
    }

    @GET
    @Path("content/{nodeid}/{objectid}/{versionid}/{fileid}")
    public Response getFile(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @PathParam("fileid") String fileID,
            @DefaultValue("empty") @QueryParam("fixity") String fixityS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getFileType(nodeID, objectIDS, versionIDS, fileID, fixityS, cs, sc);
    }

    @GET
    @Path("cloudcontainer/{containername}")
    //This endpoint had supported email delivery of objects
    //It is now only used for internal testing
    @Deprecated
    public Response callCloudContainer(
            @PathParam("containername") String containerName,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return getCloudStream(containerName, cs, sc);
    }

    @GET
    @Path("presign-file/{nodeid}/{key}")
    public Response callCloudPreSignFile(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("key") String key,
            @DefaultValue("240") @QueryParam("timeout") String expireMinutesS,
            @DefaultValue("") @QueryParam("contentType") String contentType,
            @DefaultValue("") @QueryParam("contentDisposition") String contentDisp,
            @DefaultValue("json") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            
            return cloudPreSignFile(
                nodeIDS,
                key,
                expireMinutesS,
                contentType,
                contentDisp,
                cs,
                sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, "xml", logger);

        } 
    }
    
    /**
     * This form of addVersion is specific to Ingest
     */
    @POST
    @Path("add/{nodeid}/{objectid}")
    @Consumes("application/x-www-form-urlencoded")
    public Response addVersionForm(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("") @FormParam("local-context") String localContext,
            @DefaultValue("") @FormParam("local-identifier") String localID,
            @DefaultValue("") @FormParam("manifest") String manifestRequest,
            @DefaultValue("") @FormParam("url") String url,
            @DefaultValue("") @FormParam("size") String sizeS,
            @DefaultValue("") @FormParam("digest-type") String digestType,
            @DefaultValue("") @FormParam("digest-value") String digestValue,
            @DefaultValue("xhtml") @FormParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return addVersion(
                nodeID,
                objectIDS,
                localContext,
                localID,
                manifestRequest,
                url,
                sizeS,
                digestType,
                digestValue,
                formatType,
                cs,
                sc);
    }


    @POST
    @Path("addold/{nodeid}/{objectid}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response addVersionMultipart(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("") @FormDataParam("local-context") String localContext,
            @DefaultValue("") @FormDataParam("local-identifier") String localID,
            @DefaultValue("") @FormDataParam("manifest") String manifestRequest,
            @DefaultValue("") @FormDataParam("url") String url,
            @DefaultValue("") @FormDataParam("size") String sizeS,
            @DefaultValue("") @FormDataParam("digest-type") String digestType,
            @DefaultValue("") @FormDataParam("digest-value") String digestValue,
            @DefaultValue("xhtml") @FormDataParam("response-form") String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "addVersionMultipart entered"
                    + " - localContext=" + localContext + NL
                    + " - localID=" + localID + NL
                    + " - manifestRequest=" + manifestRequest + NL
                    + " - url=" + url + NL
                    + " - sizeS=" + sizeS + NL
                    + " - digestType=" + digestType + NL
                    + " - digestValue=" + digestValue + NL
                    + " - formatType=" + formatType + NL
                    );
        if (DEBUG) System.out.println("addVersionMultipart entered");
        int nodeID = getNodeID(nodeIDS);
        return addVersion(
                nodeID,
                objectIDS,
                localContext,
                localID,
                manifestRequest,
                url,
                sizeS,
                digestType,
                digestValue,
                formatType,
                cs,
                sc);
    }

    @POST
    @Path("add/{nodeid}/{objectid}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response addVersionMultipartSpec(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("") @FormDataParam("localContext") String localContext,
            @DefaultValue("") @FormDataParam("localIdentifier") String localID,
            @DefaultValue("") @FormDataParam("manifest") String manifestRequest,
            @DefaultValue("") @FormDataParam("url") String url,
            @DefaultValue("") @FormDataParam("size") String sizeS,
            @DefaultValue("") @FormDataParam("digestType") String digestType,
            @DefaultValue("") @FormDataParam("digestValue") String digestValue,
            @DefaultValue("xhtml") @FormDataParam("responseForm") String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "addVersionMultipart entered"
                    + " - localContext=" + localContext + NL
                    + " - localID=" + localID + NL
                    + " - manifestRequest=" + manifestRequest + NL
                    + " - url=" + url + NL
                    + " - sizeS=" + sizeS + NL
                    + " - digestType=" + digestType + NL
                    + " - digestValue=" + digestValue + NL
                    + " - formatType=" + formatType + NL
                    );
        if (DEBUG) System.out.println("addVersionMultipart entered");
        int nodeID = getNodeID(nodeIDS);
        return addVersion(
                nodeID,
                objectIDS,
                localContext,
                localID,
                manifestRequest,
                url,
                sizeS,
                digestType,
                digestValue,
                formatType,
                cs,
                sc);
    }
    
    @POST
    @Path("copy/{node}/{objectid}")
    //Deprecated per David
    @Deprecated
    public Response copyObject(
            @PathParam("node") String nodeS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "addVersionMultipart entered"
                    + " - nodeS=" + nodeS + NL
                    + " - objectIDS=" + objectIDS + NL
                    + " - formatType=" + formatType + NL
                    );
        if (DEBUG) System.out.println("addVersionMultipart entered");
        int node = getNodeID(nodeS);
        return copyObject(node, objectIDS, formatType, cs, sc);
    }
    
    @POST
    @Path("copy/{sourceNode}/{targetNode}/{objectid}")
    public Response backupObject(
            @PathParam("sourceNode") String sourceNodeS,
            @PathParam("targetNode") String targetNodeS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "backupObject entered"
                    + " - sourceNodeS=" + sourceNodeS + NL
                    + " - targetNodeS=" + targetNodeS + NL
                    + " - objectIDS=" + objectIDS + NL
                    + " - formatType=" + formatType + NL
                    );
        if (DEBUG) System.out.println("addVersionMultipart entered");
        int sourceNode = getNodeID(sourceNodeS);
        int targetNode = getNodeID(targetNodeS);
        return copyObject(sourceNode, targetNode, objectIDS, formatType, cs, sc);
    }
    
    
    
    @POST
    @Path("replic/{sourceNode}/{targetNode}/{objectid}")
    public Response replicObject(
            @PathParam("sourceNode") String sourceNodeS,
            @PathParam("targetNode") String targetNodeS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "backupObject entered"
                    + " - sourceNodeS=" + sourceNodeS + NL
                    + " - targetNodeS=" + targetNodeS + NL
                    + " - objectIDS=" + objectIDS + NL
                    + " - formatType=" + formatType + NL
                    );
        if (DEBUG) System.out.println("addVersionMultipart entered");
        int sourceNode = getNodeID(sourceNodeS);
        int targetNode = getNodeID(targetNodeS);
        return replicObject(sourceNode, targetNode, objectIDS, formatType, cs, sc);
    }
    
    /**
     * This form of updateVersion is specific to Ingest
     */
    @POST
    @Path("update/{nodeid}/{objectid}")
    @Consumes("application/x-www-form-urlencoded")
    public Response updateVersionForm(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("") @FormParam("local-context") String localContext,
            @DefaultValue("") @FormParam("local-identifier") String localID,
            @DefaultValue("") @FormParam("manifest") String manifestRequest,
            @DefaultValue("") @FormParam("url") String url,
            @DefaultValue("") @FormParam("delete") String delete,
            @DefaultValue("") @FormParam("size") String sizeS,
            @DefaultValue("") @FormParam("digest-type") String digestType,
            @DefaultValue("") @FormParam("digest-value") String digestValue,
            @DefaultValue("xhtml") @FormParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "updateVersionForm entered"
                    + " - localContext=" + localContext + NL
                    + " - localID=" + localID + NL
                    + " - manifestRequest=" + manifestRequest + NL
                    + " - manifestRequest=" + delete + NL
                    + " - url=" + url + NL
                    + " - sizeS=" + sizeS + NL
                    + " - digestType=" + digestType + NL
                    + " - digestValue=" + digestValue + NL
                    + " - formatType=" + formatType + NL
                    );
        if (DEBUG) System.out.println("addVersionMultipart entered");
        int nodeID = getNodeID(nodeIDS);
        return updateVersion(
                nodeID,
                objectIDS,
                localContext,
                localID,
                manifestRequest,
                delete,
                url,
                sizeS,
                digestType,
                digestValue,
                formatType,
                cs,
                sc);
    }

    /**
     * Perform an update call to Storage
     * @param nodeIDS node identifier
     * @param objectIDS object identifier
     * @param localContext context for localID
     * @param localID local identifier
     * @param manifestRequest manifest as string with line delimiters
     * @param url URL to manifest
     * @param delete line delimited list of fileids for files to be deleted
     * @param sizeS manifest file size
     * @param digestType digest type for manifest
     * @param digestValue digest value for manifest
     * @param formatType response format type
     * @param cs
     * @param sc
     * @return Version State
     * @throws TException 
     */
    @POST
    @Path("update/{nodeid}/{objectid}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response updateVersionMultipartSpec(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("") @FormDataParam("localContext") String localContext,
            @DefaultValue("") @FormDataParam("localIdentifier") String localID,
            @DefaultValue("") @FormDataParam("manifest") String manifestRequest,
            @DefaultValue("") @FormDataParam("url") String url,
            @DefaultValue("") @FormDataParam("delete") String delete,
            @DefaultValue("") @FormDataParam("size") String sizeS,
            @DefaultValue("") @FormDataParam("digestType") String digestType,
            @DefaultValue("") @FormDataParam("digestValue") String digestValue,
            @DefaultValue("xhtml") @FormDataParam("responseForm") String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "updateVersionMultipart entered"
                    + " - localContext=" + localContext + NL
                    + " - localID=" + localID + NL
                    + " - manifestRequest=" + manifestRequest + NL
                    + " - manifestRequest=" + delete + NL
                    + " - url=" + url + NL
                    + " - sizeS=" + sizeS + NL
                    + " - digestType=" + digestType + NL
                    + " - digestValue=" + digestValue + NL
                    + " - formatType=" + formatType + NL
                    );
        if (DEBUG) System.out.println("addVersionMultipart entered");
        int nodeID = getNodeID(nodeIDS);
        if (StringUtil.isEmpty(delete)) delete = null;
        if (StringUtil.isEmpty(url)) url = null;
        if (StringUtil.isEmpty(manifestRequest)) manifestRequest = null;
        return updateVersion(
                nodeID,
                objectIDS,
                localContext,
                localID,
                manifestRequest,
                delete,
                url,
                sizeS,
                digestType,
                digestValue,
                formatType,
                cs,
                sc);
    }

    @DELETE
    @Path("content/{nodeid}/{objectid}/{versionid}")
    public Response deleteVersion(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("xml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return deleteVersion(
                nodeID,
                objectIDS,
                versionIDS,
                formatType,
                cs,
                sc);
    }

    @DELETE
    @Path("content/{nodeid}/{objectid}")
    public Response deleteObject(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return deleteObject(
                nodeID,
                objectIDS,
                formatType,
                cs,
                sc);
    }

    @GET
    @Path("content/{nodeid}/{objectid}/{versionid}")
    public Response getVersion(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("false") @QueryParam("f") String returnIfErrorS,
            @DefaultValue("false") @QueryParam("debug") String debugResponseS,
            @DefaultValue("targz") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
            return getVersionArchiveStream(
                nodeID,
                objectIDS,
                versionIDS,
                returnIfErrorS,
                debugResponseS,
                formatType,
                cs,
                sc);
    }
    @GET
    @Path("ingestlink/{nodeid}/{objectid}/{versionid}")
    public Response getIngestLink(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("false") @QueryParam("presign") String presignS,
            @DefaultValue("false") @QueryParam("update") String updateS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getIngestLink(
                    nodeID,
                    objectIDS,
                    versionIDS,
                    presignS,
                    updateS,
                    cs,
                    sc);
    }

    @GET
    @Path("versionlink/{nodeid}/{objectid}/{versionid}")
    public Response getVersionLink(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("false") @QueryParam("presign") String presignS,
            @DefaultValue("false") @QueryParam("update") String updateS,
            @DefaultValue("none") @QueryParam("filter") String filterS,
            @DefaultValue("json") @QueryParam("format") String formatS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getVersionLink(
                    nodeID,
                    objectIDS,
                    versionIDS,
                    presignS,
                    updateS,
                    filterS,
                    formatS,
                    cs,
                    sc);
    }

    @GET
    @Path("stream/{nodeid}/{objectid}/{versionid}")
    public Response getVersionStream(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("false") @QueryParam("f") String returnIfErrorS,
            @DefaultValue("false") @QueryParam("debug") String debugResponseS,
            @DefaultValue("targz") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getVersionArchiveStream(
                nodeID,
                objectIDS,
                versionIDS,
                returnIfErrorS,
                debugResponseS,
                formatType,
                cs,
                sc);
    }
    
    @POST
    @Path("fix")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response callFixObjectMultipart(
            @DefaultValue("") @FormDataParam("collection") String collection,
            @DefaultValue("") @FormDataParam("node") String nodeIDS,
            @DefaultValue("") @FormDataParam("ark") String objectIDS,
            @DefaultValue("") @FormDataParam("type") String fixType,
            @DefaultValue("false") @QueryParam("exec") String execS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        log4j.debug("FIX"
                    + " - collection=" + collection
                    + " - nodeID=" + nodeID
                    + " - objectIDS=" + objectIDS
                    + " - fixType=" + fixType
                    + " - execS=" + execS
                    );
        
        JSONObject jsonResponse = fixObject(
            collection,
            nodeID,
            objectIDS,
            fixType,
            execS,
            cs,
            sc);

            return Response 
                .status(200).entity(jsonResponse.toString())
                    .build();
    }
    
    @GET
    @Path("producer/{nodeid}/{objectid}/{versionid}")
    public Response getProducerVersion(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("false") @QueryParam("f") String returnIfErrorS,
            @DefaultValue("targz") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        
            
            System.out.println("!!!!" + MESSAGE
                    + " - nodeIDS,=" + nodeIDS
                    + " - objectIDS=" + objectIDS
                    + " - returnIfErrorS=" + returnIfErrorS
                    + " - formatType=" + formatType
                    );
            
            return getProducerVersion(
                nodeID,
                objectIDS,
                versionIDS,
                returnIfErrorS,
                formatType,
                cs,
                sc);
    }

    

    @GET
    @Path("producer/{nodeid}/{objectid}")
    public Response getProducerVersionCurrent(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("false") @QueryParam("f") String returnIfErrorS,
            @DefaultValue("targz") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        
            
            System.out.println("!!!!" + MESSAGE
                    + " - nodeIDS,=" + nodeIDS
                    + " - objectIDS=" + objectIDS
                    + " - returnIfErrorS=" + returnIfErrorS
                    + " - formatType=" + formatType
                    );
            
            return getProducerVersion(
                nodeID,
                objectIDS,
                "0",
                returnIfErrorS,
                formatType,
                cs,
                sc);
    }

    @GET
    @Path("content/{nodeid}/{objectid}")
    public Response getObject(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("true") @QueryParam("X") String returnFullVersionS,
            @DefaultValue("false") @QueryParam("f") String returnIfErrorS,
            @DefaultValue("false") @QueryParam("debug") String debugResponseS,
            @DefaultValue("targz") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        System.out.println("!!!!" + MESSAGE
                    + " - returnFullVersionS=" + returnFullVersionS
                    + " - returnIfErrorS=" + returnIfErrorS
                    );
        return getObjectArchiveStreamingResponse(
            nodeID,
            objectIDS,
            returnFullVersionS,
            returnIfErrorS,
            debugResponseS,
            formatType,
            cs,
            sc);
    }

    @GET
    @Path("stream/{nodeid}/{objectid}")
    //Deprecated per David
    @Deprecated
    public Response getObjectStream(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("true") @QueryParam("X") String returnFullVersionS,
            @DefaultValue("false") @QueryParam("f") String returnIfErrorS,
            @DefaultValue("false") @QueryParam("debug") String debugResponseS,
            @DefaultValue("targz") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @DefaultValue("by-value") @QueryParam("r") String responseMode,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getObjectArchiveStreamingResponse(
            nodeID,
            objectIDS,
            returnFullVersionS,
            returnIfErrorS,
            debugResponseS,
            formatType,
            cs,
            sc);
    }
    
    @GET
    @Path("manifest/{nodeid}/{objectid}")
    public Response getCloudManifest(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("false") @QueryParam("validate") String validateS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        int nodeID = getNodeID(nodeIDS);
        return getCloudManifestStream(
                    nodeID,
                    objectIDS,
                    validateS,
                    cs,
                    sc);
    }
    
    @POST
    @Path("assemble-obj/{nodeid}/{objectid}")
    public Response setAssembleObject(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("zip") @QueryParam("format") String archiveTypeS,
            @DefaultValue("false") @QueryParam("returnIfError") String returnOnErrorS,
            @DefaultValue("false") @QueryParam("full-version") String returnFullVersionS,
            @DefaultValue("") @QueryParam("assemble-node") String assembleNodeS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        System.out.println("setAssembleObject entered");
        String archiveContentS = "full";
        return setAssembleArchiveAsync(
            nodeIDS,
            objectIDS,
            null,
            archiveTypeS,
            archiveContentS,
            returnOnErrorS,
            returnFullVersionS,
            assembleNodeS,
            cs,
            sc);
    }
    
    @POST
    @Path("assemble-obj/{nodeid}/{objectid}/{versionid}")
    public Response setAssembleVersion(
            @PathParam("nodeid") String nodeIDS,
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @DefaultValue("zip") @QueryParam("format") String archiveTypeS,
            @DefaultValue("full") @QueryParam("content") String content,
            @DefaultValue("false") @QueryParam("returnIfError") String returnOnErrorS,
            @DefaultValue("false") @QueryParam("full-version") String returnFullVersionS,
            @DefaultValue("") @QueryParam("assemble-node") String assembleNodeS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return setAssembleArchiveAsync(
            nodeIDS,
            objectIDS,
            versionIDS,
            archiveTypeS,
            content,
            returnOnErrorS,
            returnFullVersionS,
            assembleNodeS,
            cs,
            sc);
    }
    
    @GET
    @Path("presign-obj-by-token/{asynchtoken}")
    public Response getPresignAsynchObject(
            @PathParam("asynchtoken") String asynchToken,
            @DefaultValue("240") @QueryParam("timeout") String expireMinutesS,
            @DefaultValue("") @QueryParam("assemble-node") String assembleNodeS,
            @DefaultValue("") @QueryParam("contentDisposition") String contentDisposition,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return doPresignAsynchObject(
            asynchToken,
            expireMinutesS,
            assembleNodeS,
            contentDisposition,
            cs,
            sc);
    }
    
    @GET
    @Path("hostname")
    public Response getHostname(
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return getHostnameServ(
            cs,
            sc);
    }
    
}
