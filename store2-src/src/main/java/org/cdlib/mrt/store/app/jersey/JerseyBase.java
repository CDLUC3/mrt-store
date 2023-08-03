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
package org.cdlib.mrt.store.app.jersey;


import org.glassfish.jersey.server.CloseableService;
import org.cdlib.mrt.store.app.ValidateCmdParms;
import org.cdlib.mrt.store.app.*;
import java.net.InetAddress;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import javax.servlet.ServletConfig;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.MediaType;

import javax.ws.rs.core.Response;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.cloud.utility.CloudUtil;

import org.cdlib.mrt.cloud.action.ContentVersionLink;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.utility.PairtreeUtil;
import org.cdlib.mrt.store.action.AsyncContainerObject;
import org.cdlib.mrt.store.storage.StorageServiceInf;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.PreSignedState;
import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.store.TokenGetState;
import org.cdlib.mrt.store.TokenPostState;
import org.cdlib.mrt.store.TokenStatus;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.store.tools.JSONTools;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.ArchiveBuilderBase;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.log.utility.AddStateEntry;
import org.cdlib.mrt.store.logging.LogEntryObject;
import org.cdlib.mrt.store.logging.LogEntryVersion;
import org.cdlib.mrt.store.action.AsyncCloudArchive;
import org.cdlib.mrt.store.action.TokenManager;
import org.cdlib.mrt.store.consumer.utility.QueueUtil;
import org.cdlib.mrt.store.logging.LogEntryTokenStatus;
import org.cdlib.mrt.store.tools.FileFromUrl;
import org.cdlib.mrt.store.zoo.ZooTokenHandler;
import org.cdlib.mrt.store.zoo.ZooTokenState;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.SerializeUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.json.JSONObject;
/**
 * Base Jersey handling for both Storage and CAN services
 * The attempt is to keep the Jersey layer as thin as possible.
 * Jersey provides the servlet layer for storage RESTful interface
 * <pre>
 * The Jersey routines typically perform the following functions:
 * - get System configuration
 * - get StorageManager
 * - call appropriate StorageManager method
 * - return file or create formatted file
 * - encapsolate formatted file in Jersey Response - setting appropriate return codes
 * </pre>
 * @author dloy
 */
public class JerseyBase
{

    protected static final String NAME = "JerseyBase";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT
            = FormatterInf.Format.xml;
    protected static final boolean DEBUG = false;
    protected static final String NL = System.getProperty("line.separator");

    protected LoggerInf defaultLogger = new TFileLogger("Jersey", 10, 10);
    protected JerseyCleanup jerseyCleanup = new JerseyCleanup();
    
    protected static final Logger log4j = LogManager.getLogger();



    /**
     * Shortcut enum for format types for both State display and Archive response
     */
    public enum FormatType
    {
        anvl("state", "txt", "text/x-anvl", null),
        json("state", "json", "application/json", null),
        serial("state", "ser", "application/x-java-serialized-object", null),
        octet("file", "txt", "application/octet-stream", null),
        targz("archive", "tar.gz", "application/x-tar-gz", "gzip"),
        tar("archive", "tar", "application/x-tar", null),
        txt("file", "txt", "plain/text", null),
        xml("state", "xml", "text/xml", null),
        rdf("state", "xml", "application/rdf+xml", null),
        turtle("state", "ttl", "text/turtle", null),
        xhtml("state", "xhtml", "application/xhtml+xml", null),
        zipunc("archive", "zip", "application/zip", null),
        zip("archive", "zip", "application/zip", null);

        protected final String form;
        protected final String extension;
        protected final String mimeType;
        protected final String encoding;

        FormatType(String form, String extension, String mimeType, String encoding) {
            this.form = form;
            this.extension = extension;
            this.mimeType = mimeType;
            this.encoding = encoding;
        }

        /**
         * Extension for this format
         * @return
         */
        public String getExtension() {
            return extension;
        }

        /**
         * return MIME type of this format response
         * @param t
         * @return MIME type
         */
        public String getMimeType() {
            return mimeType;
        }

        /**
         * return form of this format
         * @param t
         * @return MIME type
         */
        public String getForm() {
            return form;
        }

        /**
         * return encoding of this format
         * @return encoding
         */
        public String getEncoding() {
            return encoding;
        }

        public static FormatType containsExtension(String t)
        {
            if (StringUtil.isEmpty(t)) return null;
            for (FormatType p : FormatType.values()) {
                if (t.contains("." + p.getExtension())) {
                    return p;
                }
            }
            return null;
        }

        public static FormatType valueOfExtension(String t)
        {
            if (StringUtil.isEmpty(t)) return null;
            for (FormatType p : FormatType.values()) {
                if (p.getExtension().equals(t)) {
                    return p;
                }
            }
            return null;
        }

        /**
         * return MIME type of this format response
         * @param t
         * @return MIME type
         */
        public static FormatType valueOfMimeType(String t)
        {
            if (StringUtil.isEmpty(t)) return null;
            for (FormatType p : FormatType.values()) {
                if (p.getMimeType().equals(t)) {
                    return p;
                }
            }
            return null;
        }
    }
     
    /**
     * Format file from input State file
     *  - identify format type
     *  - format the State object (locally or remote)
     *  - save formatted object to file
     *  - return file
     * @param responseState object to be formatted
     * @param formatType user requested format type
     * @param logger file logger
     * @return formatted data with MimeType
     * @throws TException
     */
    protected TypeFile getStateFile(StateInf responseState, FormatType outputFormat, LoggerInf logger)
            throws TException
    {
        if (responseState == null) return null;
        PrintStream stream = null;
        TypeFile typeFile = new TypeFile();
        //System.out.println("!!!!" + MESSAGE + "getStateFile localFormat=" + localFormat.toString());
        try {
            if (outputFormat == FormatType.serial) {
                typeFile.formatType = outputFormat;
                if (responseState instanceof Serializable) {
                    Serializable serial = (Serializable)responseState;
                    typeFile.file = FileUtil.getTempFile("state", ".ser");
                    SerializeUtil.serialize(serial, typeFile.file);
                }
            }

            if (typeFile.file == null) {
                FormatterInf formatter = getFormatter(outputFormat, logger);
                FormatterInf.Format formatterType = formatter.getFormatterType();
                String foundFormatType = formatterType.toString();
                typeFile.formatType = FormatType.valueOf(foundFormatType);
                String ext = typeFile.formatType.getExtension();
                typeFile.file = FileUtil.getTempFile("state", "." + ext);
                FileOutputStream outStream = new FileOutputStream(typeFile.file);
                stream = new PrintStream(outStream, true, "utf-8");
                formatter.format(responseState, stream);
            }
            return typeFile;

        } catch (TException tex) {
            System.out.println("Stack:" + StringUtil.stackTrace(tex));
            throw tex;

        } catch (Exception ex) {
            System.out.println("Stack:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + " Exception:" + ex);

        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception ex) { }
            }
        }

    }

    /**
     * Validate that the user passed format legit
     * @param formatType user passed format
     * @param form type of format: "state", "archive", "file"
     * @return FormatType form of user format
     * @throws TException
     */
    protected FormatType getFormatType(String formatType, String form)
            throws TException
    {
        try {
            if (StringUtil.isEmpty(formatType)) {
                throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
            }
            formatType = formatType.toLowerCase();
            FormatType format = FormatType.valueOf(formatType);
            if (!format.getForm().equals(form)) {
                throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
            }

        return format;
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
        }
    }
    
    /**
     * Get monitor information for Storage
     * @param gcS
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted monitor content
     * @throws TException 
     */
    public Response getPingState(
            String gcS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("getPingState entered:"
                    + " - gc=" + gcS
                    + " - formatType=" + formatType
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = storageService.getLogger();
            boolean gc = setBool(gcS, false, false, false);
            StateInf responseState = storageService.getPingState(gc);
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

    /**
     * Get state information about a specific node
     * @param nodeID node identifier
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted node state information
     * @throws TException
     */
    public Response getNodeState(
            int nodeID,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getNodeState entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getNodeState(nodeID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Get state information about a specific node
     * @param nodeID node identifier
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted node state information
     * @throws TException
     */
    public Response findObjectState(
            Identifier objectID,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getNodeState entered:"
                    + " - formatType=" + formatType
                    + " - objectIDS=" + objectID.getValue()
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            StateInf responseState = storageService.findObjectState(objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Get state information about a specific Object
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted object state information
     * @throws TException
     */
    public Response getObjectState(
            int nodeID,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            Identifier objectID = getObjectID(objectIDS);
            log("getNodeState entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getObjectState(nodeID, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

         } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Get state information about a specific Object
     * @param nodeIDs array of nodes
     * @param objectIDS object identifier
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted object state information
     * @throws TException
     */
    public Response getObjectState(
            int [] nodeIDs,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        int nodeID = 0;
        StateInf responseState = null;
        try {
            Identifier objectID = getObjectID(objectIDS);
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            TException.REQUESTED_ITEM_NOT_FOUND rinfSave = null;
            LoggerInf [] loggerList = new LoggerInf[nodeIDs.length];
            // validate all nodes
            for (int inode=0; inode < nodeIDs.length; inode++) {
                try {
                    nodeID = nodeIDs[inode];
                    log("getObjectState entered:"
                        + " - formatType=" + formatType
                        + " - nodeId[" + inode + "]=" + nodeID
                        );
                    loggerList[inode] = getNodeLogger(nodeID, storageService);
                } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                    throw new TException.INVALID_OR_MISSING_PARM ("Required node not found:" + nodeID);
                }
            }
            for (int inode=0; inode < nodeIDs.length; inode++) {
                nodeID = nodeIDs[inode];
                log("getObjectState entered:"
                    + " - formatType=" + formatType
                    + " - nodeId[" + inode + "]=" + nodeID
                    );
                logger = loggerList[inode];
                try {
                    responseState = storageService.getObjectState(nodeID, objectID);
                    rinfSave = null;
                    break;
                    
                } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                    rinfSave = rinf;
                }
            }
            if (rinfSave != null) {
                throw rinfSave;
            }
            return getStateResponse(responseState, formatType, logger, cs, sc);

         } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

 
    /**
     * Get state information about a specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier for state information
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response getVersionStateProcess(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            log("getVersionState entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getVersionState(nodeID, objectID, versionID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
     * Get file state information
     * @param nodeID node identifier
     * @param objectIDS object identifier
     * @param versionIDS version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileID file name
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response getFileState(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String fileID,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            fileID = getFileID(fileID);
            log("getFileState entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - fileID=" + fileID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getFileState(nodeID, objectID, versionID, fileID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
     * Perform a fixity test on a specific node-object-version-file
     * @param nodeID node identifier
     * @param objectIDS object identifier
     * @param versionIDS version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileID file name
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response getFileFixityState(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String fileID,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            fileID = getFileID(fileID);
            log4j.debug("getFileFixityState entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - fileID=" + fileID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getFileFixityState(nodeID, objectID, versionID, fileID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Perform a fixity test on a specific node-object-version-file
     * @param nodeID node identifier
     * @param objectIDS object identifier
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response getObjectFixityState(
            int nodeID,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            Identifier objectID = getObjectID(objectIDS);
            log4j.debug("getObjectFixityState entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getObjectFixityState(nodeID, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Get a specific file for a node-object-version
     * @param nodeID node identifier
     * @param objectIDS object identifier
     * @param versionIDS version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileID file name
     * @param fixityS should fixity be performed
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response getFileType(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String fileID,
            String fixityS,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        if (fixityS.equals("empty")) fixityS = null;
        LoggerInf logger = defaultLogger;
        boolean doFixity = false;
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            fileID = getFileID(fileID);
            log4j.debug("getFileFixityState entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - fileID=" + fileID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            NodeState nodeState = storageService.getNodeState(nodeID);
            boolean isVerifyOnRead = nodeState.isVerifyOnRead();
            Boolean fixity = setBoolean(fixityS);
            if (fixity == null) {
                if (isVerifyOnRead) {
                    doFixity = true;
                } else {
                    doFixity = false;
                }
            } else {
                if (isVerifyOnRead) {
                    doFixity = true;
                    if (!fixity) doFixity = false;
                } else {
                    doFixity = false;
                    if (fixity) doFixity = true;
                }
            }
            if (doFixity) {
                return getFile(nodeID, objectID, versionID, fileID, cs, storageService);
                
            } else {
                return getFileStream(nodeID, objectID, versionID, fileID, cs, storageService);
            }

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, "xml", logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
        
    /**
     * Get a specific file for a node-object-version
     * @param nodeID node identifier
     * @param objectIDS object identifier
     * @param versionIDS version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileID file name
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return content based on key
     * @throws TException 
     */
    public Response getCloudStream(
            String key,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getCloudContainer entered:"
                    + " - key=" + key
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            StorageConfig storageConfig = storageService.getStorageConfig();
            Long outNode = storageConfig.getArchiveNode();
            NodeIO.AccessNode accessNode = storageConfig.getAccessNode(outNode);
            if (accessNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "NodeIO.AccessNode not found - outNode:" + outNode);
            }
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            Properties cloudProp = service.getObjectMeta(bucket, key);
            if (cloudProp == null) {
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "call to service fails:"
                        + " - outNode:" + outNode
                        + " - bucket:" + bucket
                        + " - key:" + key
                );
            }
            if (cloudProp.size() == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "container item not found:"
                        + " - outNode:" + outNode
                        + " - bucket:" + bucket
                        + " - key:" + key
                );
            }
            System.out.println("getCloudStream" + PropertiesUtil.dumpProperties(key, cloudProp));
            
            CloudStreamingOutput streamingOutput = new CloudStreamingOutput(service, bucket, key);
            String fileResponseName = getFileResponseFileName(key);
            return getFileResponseEntity(streamingOutput, fileResponseName);
            
        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    /**
     * Get a specific file for a node-object-version
     * @param nodeID node identifier
     * @param objectIDS object identifier
     * @param versionIDS version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileID file name
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response getFile(
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileID,
            CloseableService cs,
            StorageServiceInf storageService)
        throws TException
    {

        LoggerInf logger = null;
        try {
            fileID = getFileID(fileID);
            String formatType = "octet";
            log("getFile entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - fileID=" + fileID
                    );
            logger = getNodeLogger(nodeID, storageService);
            FileContent content = storageService.getFile(nodeID, objectID, versionID, fileID);
            String fileResponseName = getFileResponseFileName(fileID);
            return getFileResponse(content, formatType, fileResponseName, cs, logger);

        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);

            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Get a specific file for a node-object-version
     * @param nodeID node identifier
     * @param objectIDS object identifier
     * @param versionIDS version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileID file name
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response getFileStream(
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileID,
            CloseableService cs,
            StorageServiceInf storageService)
        throws TException
    {
        

        LoggerInf logger = null;
        try {
            fileID = getFileID(fileID);
            log("getFileStream entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileID=" + fileID
                    );
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getFileState(nodeID, objectID, versionID, fileID);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, "xml", logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
        try {
            
            ComponentStreamingOutput streamingOutput = new ComponentStreamingOutput(
                nodeID,
                objectID,
                versionID,
                fileID,
                storageService
                );
            String fileResponseName = getFileResponseFileName(fileID);
            return getFileResponseEntity(streamingOutput, "xml", fileResponseName);
            
        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Add an object to this storage service
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param context authorize context
     * @param localID local identifier
     * @param manifestRequest manifest as String
     * @param url link to manifest
     * @param sizeS size of manifest
     * @param type manifest digest type (checksumtype)
     * @param value manifest digest value (checksum)
     * @param formatType user provided format type
     * @param cs close service
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response addVersion(
            int nodeID,
            String objectIDS,
            String localContext,
            String localID,
            String manifestRequest,
            String url,
            String sizeS,
            String digestType,
            String digestValue,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        File manifest = null;
        
        try {
            log4j.debug("addVersion entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectIDS=" + objectIDS
                    + " - localContext=" + localContext
                    + " - localID=" + localID
                    + " - manifest=" + manifestRequest
                    + " - url=" + url
                    + " - sizeS=" + sizeS
                    + " - digestType=" + digestType
                    + " - digestValue=" + digestValue
                    + " - t=" + formatType
                    );

            sizeS = StringUtil.normParm(sizeS);
            digestType = StringUtil.normParm(digestType);
            digestValue = StringUtil.normParm(digestValue);
            localContext = StringUtil.normParm(localContext);
            localID = StringUtil.normParm(localID);
            formatType = StringUtil.normParm(formatType);
            if (StringUtil.isEmpty(formatType)) formatType = "xml";

            Identifier objectID = getObjectID(objectIDS);
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            manifest = getManifest(manifestRequest, url, logger);
            validateManifest(manifest, sizeS, digestType, digestValue);
            
            //jerseyCleanup.addTempFile(manifest);
            long startTime = System.currentTimeMillis();
            VersionState responseState = addVersion(nodeID, objectID, localContext, localID, manifest, storageService, logger);
            long nodeL = nodeID;
            LogEntryVersion logEntry = LogEntryVersion.getLogEntryVersion("StoreAdd",
                    nodeL,
                    System.currentTimeMillis() - startTime, 
                    responseState);
            logEntry.addEntry();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
            
        } finally {
            FileFromUrl.delete(manifest);
        }
    }
    
    public Response copyObject(
            int node,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {        LoggerInf logger = defaultLogger;
        try {
            log4j.debug("copyObject entered:"
                    + " - formatType=" + formatType
                    + " - node=" + node
                    + " - objectIDS=" + objectIDS
                    + " - t=" + formatType
                    );

            formatType = StringUtil.normParm(formatType);
            if (StringUtil.isEmpty(formatType)) formatType = "xml";

            Identifier objectID = getObjectID(objectIDS);
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(node, storageService);
            StateInf responseState = storageService.copyObject(node, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response copyObject(
            int sourceNode,
            int targetNode,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {        LoggerInf logger = defaultLogger;
        try {
            log4j.debug("copyObject entered:"
                    + " - formatType=" + formatType
                    + " - sourceNode=" + sourceNode
                    + " - targetNode=" + targetNode
                    + " - objectIDS=" + objectIDS
                    + " - t=" + formatType
                    );

            formatType = StringUtil.normParm(formatType);
            if (StringUtil.isEmpty(formatType)) formatType = "xml";

            Identifier objectID = getObjectID(objectIDS);
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(sourceNode, storageService);
            StateInf responseState = storageService.copyObject(sourceNode, targetNode, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }    

    /**
     * Update an object to this storage service
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param context authorize context
     * @param localID local identifier
     * @param manifestRequest manifest as String
     * @param url link to manifest
     * @param sizeS size of manifest
     * @param type manifest digest type (checksumtype)
     * @param value manifest digest value (checksum)
     * @param formatType user provided format type
     * @param cs close service
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response updateVersion(
            int nodeID,
            String objectIDS,
            String localContext,
            String localID,
            String manifestRequest,
            String delete,
            String url,
            String sizeS,
            String digestType,
            String digestValue,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        File manifest = null;
        try {
            log4j.debug("updateVersion entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectIDS=" + objectIDS
                    + " - localContext=" + localContext
                    + " - localID=" + localID
                    + " - manifest=" + manifestRequest
                    + " - url=" + url
                    + " - sizeS=" + sizeS
                    + " - digestType=" + digestType
                    + " - digestValue=" + digestValue
                    + " - t=" + formatType
                    );

            sizeS = StringUtil.normParm(sizeS);
            digestType = StringUtil.normParm(digestType);
            digestValue = StringUtil.normParm(digestValue);
            localContext = StringUtil.normParm(localContext);
            localID = StringUtil.normParm(localID);
            formatType = StringUtil.normParm(formatType);
            if (StringUtil.isEmpty(formatType)) formatType = "xml";

            Identifier objectID = getObjectID(objectIDS);
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            
            if ((manifestRequest != null) || (url != null)) {
                manifest = getManifest(manifestRequest, url, logger);
                validateManifest(manifest, sizeS, digestType, digestValue);
                //jerseyCleanup.addTempFile(manifest);
            }
            String [] deleteList = null;
            if (StringUtil.isEmpty(delete)) deleteList = null;
            else {
                delete = delete.trim();
                deleteList = delete.split("\\r?\\n");
            }
            
            long startTime = System.currentTimeMillis();
            VersionState responseState = updateVersion(nodeID, objectID, localContext, localID, manifest, deleteList, storageService, logger);
            long nodeL = nodeID;
            LogEntryVersion logEntry = LogEntryVersion.getLogEntryVersion("StoreUpdate",
                    nodeL,
                    System.currentTimeMillis() - startTime, 
                    responseState);
            logEntry.addEntry();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
            
        } finally {
            FileFromUrl.delete(manifest);
        }
    }

    protected void validateManifest(
            File manifest,
            String sizeS,
            String type,
            String value)
        throws TException
    {
        long size = 0;
        if (StringUtil.isNotEmpty(sizeS)) {
            try {
                size = Long.parseLong(sizeS);
            } catch (Exception ex) {
                throw new TException.REQUEST_INVALID(
                        "validateManifest - manifest size not numeric:"
                        + sizeS);
            }
            if (manifest.length() != size) {
                throw new TException.INVALID_DATA_FORMAT(
                        "validateManifest - size argument does not match passed manifest"
                        + " - passed size=" + size
                        + " - manifest size=" + manifest.length()
                        );
            }
        }
        if (size == 0) size = manifest.length();
        if (StringUtil.isNotEmpty(type)
                && StringUtil.isNotEmpty(value)) {
            LoggerInf logger = new TFileLogger("JerseyStorage", 10, 10);
            FixityTests fixity = new FixityTests(manifest, type, logger);
            FixityTests.FixityResult fixityResult = fixity.validateSizeChecksum(value, type, size);
            if (!fixityResult.fileSizeMatch || !fixityResult.checksumMatch) {
                String msg = MESSAGE + "validateManifest - Fixity fails."
                        + fixityResult.dump("");
                throw new TException.INVALID_DATA_FORMAT(msg);
            }
        }
    }

    /**
     * Add an object to this storage service
     * @param nodeID nodeID node identifier for object
     * @param objectID object identifier
     * @param manifestFile manifest as File
     * @param sc ServletConfig used to get system configuration
     * @return version state information for added item
     * @throws TException processing exception
     */
    protected VersionState addVersion(
            int nodeID,
            Identifier objectID,
            String localContext,
            String localID,
            File manifestFile,
            StorageServiceInf storageService,
            LoggerInf logger)
        throws TException
    {
        try {
            VersionState responseState = storageService.addVersion(nodeID, objectID, localContext, localID, manifestFile);
            return responseState;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Add an object to this storage service
     * @param nodeID nodeID node identifier for object
     * @param objectID object identifier
     * @param manifestFile manifest as File
     * @param sc ServletConfig used to get system configuration
     * @return version state information for added item
     * @throws TException processing exception
     */
    protected VersionState updateVersion(
            int nodeID,
            Identifier objectID,
            String localContext,
            String localID,
            File manifestFile,
            String[] deleteList,
            StorageServiceInf storageService,
            LoggerInf logger)
        throws TException
    {
        try {
            VersionState responseState = storageService.updateVersion(nodeID, objectID, localContext, localID, manifestFile, deleteList);
            return responseState;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Remove current version of an object
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param versionID version identifier
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response deleteVersion(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("deleteVersion entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectIDS=" + objectIDS
                    + " - versionID=" + versionIDS
                    + " - t=" + formatType
                    );
            Identifier objectID = getObjectID(objectIDS);
            int versionID = getVersionID(versionIDS);
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = deleteVersion(nodeID, objectID, versionID, storageService, logger);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Remove object
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response deleteObject(
            int nodeID,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log4j.debug("deleteObject entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectIDS=" + objectIDS
                    + " - t=" + formatType
                    );
            Identifier objectID = getObjectID(objectIDS);
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            long startTime = System.currentTimeMillis();
            long nodeL = nodeID;
            ObjectState responseState = deleteObject(nodeID, objectID, storageService, logger);
            LogEntryObject logEntry = LogEntryObject.getLogEntryObject("StoreDelete",
                    nodeL,
                    System.currentTimeMillis() - startTime, 
                    responseState);
            logEntry.addEntry();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Delete the current version of an object
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param manifestFile manifest as File
     * @param sc ServletConfig used to get system configuration
     * @return version state information for added item
     * @throws TException processing exception
     */
    protected StateInf deleteVersion(
            int nodeID,
            Identifier objectID,
            int versionID,
            StorageServiceInf storageService,
            LoggerInf logger)
        throws TException
    {
        try {
            StateInf responseState = storageService.deleteVersion(nodeID, objectID, versionID);
            return responseState;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Delete the current version of an object
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param manifestFile manifest as File
     * @param sc ServletConfig used to get system configuration
     * @return version state information for added item
     * @throws TException processing exception
     */
    protected ObjectState deleteObject(
            int nodeID,
            Identifier objectID,
            StorageServiceInf storageService,
            LoggerInf logger)
        throws TException
    {
        try {
            ObjectState responseState = storageService.deleteObject(nodeID, objectID);
            return responseState;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param returnFullVersionS "true"=returned archive contains full expansions of delta,
     * "false"=return dflat as is
     * @param returnIfErrorS "true"=no fixity test, "false"=fixity test
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
    public Response getObjectArchive(
            int nodeID,
            String objectIDS,
            String returnFullVersionS,
            String returnIfErrorS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;

        boolean returnFullVersion = setBool(returnFullVersionS, false, true, false);
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        try {
            Identifier objectID = getObjectID(objectIDS);
            log("getObjectArchive entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            AddStateEntry logEntry = AddStateEntry.getAddStateEntry("store", "getObjectArchive")
                    .setObjectID(objectID)
                    .setSourceNode(nodeID);
            FileContent content = storageService.getObjectArchive(
                    nodeID, objectID, returnFullVersion, returnIfError, formatType);
            File containFile = content.getFile();
            if (containFile != null) {
                long fileSize = containFile.length();
                logEntry = logEntry.setBytes(fileSize);
            }
            logEntry.addLog("info", "storeJSON");
            FormatType formatTypeE = FormatType.valueOf(formatType);
            String fileResponseName = getResponseFileName(objectIDS, formatTypeE.getExtension());
            return getFileResponse(content, formatType, fileResponseName, cs, logger);

        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param returnFullVersionS "true"=returned archive contains full expansions of delta,
     * "false"=return dflat as is
     * @param returnIfErrorS "true"=no fixity test, "false"=fixity test
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
    public Response getObjectArchiveStreamingResponse(
            int nodeID,
            String objectIDS,
            String returnFullVersionS,
            String returnIfErrorS,
            String debugResponseS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        Identifier objectID = getObjectID(objectIDS);
        StorageServiceInf storageService = null;
        try {
            log("getObjectArchiveStreamingResponse entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getObjectState(nodeID, objectID);

         } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
        //validate archive type options before Jersey streaming
        String archiveTypeS = formatType.toLowerCase();
        ArchiveBuilderBase.ArchiveType archiveType = null;
        try {
            if (DEBUG) System.out.println(MESSAGE + "getObjectStream before ArchiveBuilder");
            archiveType
                    = ArchiveBuilderBase.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not recognized");
        }
        
        boolean returnFullVersion = setBool(returnFullVersionS, false, true, false);
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        boolean debugResponse = setBool(debugResponseS, false, false, false);
        try {
            ObjectStreamingOutput streamingOutput = new ObjectStreamingOutput(
                nodeID,
                objectID,
                storageService,
                returnFullVersion,
                returnIfError,
                formatType);
            FormatType formatTypeE = FormatType.valueOf(formatType);
            String responseName = getResponseFileName(objectIDS, formatTypeE.getExtension());
            if (debugResponse) {
                return getFileResponseEntityTest(streamingOutput, formatType, responseName);
            }
            return getFileResponseEntity(streamingOutput, formatType, responseName);

        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param returnFullVersionS "true"=returned archive contains full expansions of delta,
     * "false"=return dflat as is
     * @param returnIfErrorS "true"=no fixity test, "false"=fixity test
     * @param archiveTypeS user provided format type for created container
     * @param formatType user provided format type synchronized response
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */    
     public Response setObjectAsyncArchive(
            int nodeID,
            String objectIDS,
            String returnFullVersionS,
            String returnIfErrorS,
            String name,
            String email,
            String archiveTypeS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        Identifier objectID = getObjectID(objectIDS);
        StorageServiceInf storageService = null;
        StateInf responseState = null;
        try {
            log("getObjectArchiveStreamingResponse entered:"
                    + " - formatType=" + formatType
                    + " - archiveTypeS=" + archiveTypeS
                    + " - nodeId=" + nodeID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            responseState = storageService.getObjectState(nodeID, objectID);

         } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
        //validate archive type options before Jersey streaming
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilderBase.ArchiveType archiveType = null;
        try {
            if (DEBUG) System.out.println(MESSAGE + "getObjectStream before ArchiveBuilder");
            archiveType
                    = ArchiveBuilderBase.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not recognized");
        }
        
        boolean returnFullVersion = setBool(returnFullVersionS, false, true, false);
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        try {
            ObjectStreamingOutput streamingOutput = new ObjectStreamingOutput(
                nodeID,
                objectID,
                storageService,
                returnFullVersion,
                returnIfError,
                archiveTypeS);
            FormatType archiveTypeE = FormatType.valueOf(archiveTypeS);
            //Response tResponse = getFileResponseEntityTest(streamingOutput, formatType, "thisisatest.txt");
            
            Response errorResponse = setAsyncArchive(storageService, streamingOutput, name, email, formatType, logger);
            if (errorResponse != null) return errorResponse;
            return getStateResponse(responseState, formatType, logger, cs, sc);
            
        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier
     * "false"=return dflat as is
     * @param returnIfErrorS "true"=no fixity test, "false"=fixity test
     * @param archiveTypeS user provided format type for created container
     * @param formatType user provided format type synchronized response
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
     public Response setVersionAsyncArchive(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String returnIfErrorS,
            String name,
            String email,
            String archiveTypeS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        int versionID = getVersionID(versionIDS);
        Identifier objectID = getObjectID(objectIDS);
        StorageServiceInf storageService = null;
        StateInf responseState = null;
        try {
            log("setVersionAsyncArchive entered:" + NL
                    + " - nodeID=" + nodeID+ NL
                    + " - returnIfErrorS=" + returnIfErrorS+ NL
                    + " - name=" + name+ NL
                    + " - email=" + email+ NL
                    + " - archiveTypeS=" + archiveTypeS
                    + " - formatType=" + formatType
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            responseState = storageService.getVersionState(nodeID, objectID, versionID);

         } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
        //validate archive type options before Jersey streaming
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilderBase.ArchiveType archiveType = null;
        try {
            if (DEBUG) System.out.println(MESSAGE + "getObjectStream before ArchiveBuilder");
            archiveType
                    = ArchiveBuilderBase.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not recognized");
        }
        
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        try {
            VersionStreamingOutput streamingOutput = new VersionStreamingOutput(
                nodeID,
                objectID,
                versionID,
                storageService,
                returnIfError,
                archiveTypeS);
            //Response tResponse = getFileResponseEntityTest(streamingOutput, formatType, "thisisatest.txt");
            
            Response errorResponse = setAsyncArchive(storageService, streamingOutput, name, email, formatType, logger);
            if (errorResponse != null) return errorResponse;
            return getStateResponse(responseState, formatType, logger, cs, sc);
            
        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier
     * "false"=return dflat as is
     * @param returnIfErrorS "true"=no fixity test, "false"=fixity test
     * @param archiveTypeS user provided format type for created container
     * @param formatType user provided format type synchronized response
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
     public Response setAssembleArchiveAsync(
            String nodeIDS,
            String objectIDS,
            String versionIDS,
            String archiveTypeS,
            String archiveContentS,
            String returnOnErrorS,
            String returnFullVersionS,
            String assembleNodeS,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        Integer versionID = getVersionIDCond(versionIDS);
        Identifier objectID = getObjectID(objectIDS);
        StorageServiceInf storageService = null;
        try {
            Boolean fullObject = setBoolean(returnFullVersionS);
            Boolean returnOnError = setBoolean(returnOnErrorS);
            int nodeID = getNodeID(nodeIDS);
            Long assembleNode = getLongNull(assembleNodeS);
            
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            
            logger = getNodeLogger(nodeID, storageService);
            StorageConfig storageConfig = storageService.getStorageConfig();
            Long archiveNode = storageConfig.getArchiveNode();
            if (assembleNode != null) {
                archiveNode = assembleNode;
            }
            ArrayList<String> producerFilter = storageConfig.getProducerFilter();
            // if no list set to null
            if (producerFilter.size() == 0) {
                producerFilter = null;
            }
            
            TokenPostState postState = null;
            long nodeIDL = nodeID;
            TokenManager tokenManager = null;
            try {
                tokenManager = TokenManager.getNewTokenManager(
                    storageConfig.getNodeIO(),
                    nodeIDL,
                    archiveNode,
                    objectID,
                    versionID,
                    archiveTypeS,
                    fullObject,
                    returnOnError,
                    archiveContentS,
                    producerFilter,
                    logger);
                String saveJSON = tokenManager.saveCloudToken();
                System.out.println("saveJSON:" + saveJSON);
                
            } catch (TException tex) {
                System.out.println("setAssembleArchiveAsync exception:" + tex);
                tex.printStackTrace();
                postState = TokenPostState.getTokenPostState(tex);
                int status = postState.getHttpStatus();
                String json = postState.getJsonError();
                return Response 
                    .status(status).entity(json)
                    .build();
            }
            postState = TokenPostState.getTokenPostState(tokenManager.getTokenStatus());
            String jsonQueue = postState.getJsonQueue();
            System.out.println(">>>JsonQueue:" + jsonQueue);
            //!!! buildTokenAsynch(tokenManager.getTokenStatus(), logger);
            queueTokenAsynch(jsonQueue, tokenManager, logger);
            int status = postState.getHttpStatus();
            String json = postState.getJsonOK();
            
            return Response 
                    .status(status).entity(json)
                    .build();
    

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    protected void buildTokenAsynch(TokenStatus tokenStatus, LoggerInf logger)
        throws TException
    {
        try {
            AsyncCloudArchive asyncCloudArchive = AsyncCloudArchive.getAsyncCloudArchive(
                tokenStatus,
                logger);
            Thread asyncThread = new Thread(asyncCloudArchive);
            asyncThread.start();
            System.out.println(MESSAGE + "buildTokenAsynch - Start thread:" + tokenStatus.getToken());
                    
        } catch (TException tex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(tex));
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    protected void queueTokenAsynch(String jsonQueue, TokenManager tokenManager, LoggerInf logger)
        throws TException
    {
        try {
            TokenStatus tokenStatus = tokenManager.getTokenStatus();
            tokenStatus.setTokenStatusEnum("Queued");
            tokenManager.saveCloudToken();
            boolean worked = QueueUtil.queueAccessRequest(jsonQueue);
                    
        } catch (TException tex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(tex));
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    protected void setTokenZookeeper(String jsonQueue)
        throws TException
    {
        System.out.println("***setTokenZookeeper:" + jsonQueue);
    }
    
    protected Response doPresignAsynchObject(
            String asynchToken,
            String expireMinutesS,
            String assembleNodeS,
            String contentDisposition,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        TokenStatus tokenStatus = null;
        LoggerInf logger = defaultLogger;
        StorageServiceInf storageService = null;
        TokenGetState tokenGetState = null;
        if (StringUtil.isAllBlank(contentDisposition)) {
            contentDisposition = null;
        }
        try {
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            
            StorageConfig storageConfig = storageService.getStorageConfig();
            Long archiveNode = storageConfig.getArchiveNode();
            Long assembleNode = getLongNull(assembleNodeS);
            if (assembleNode != null) {
                archiveNode = assembleNode;
            }
            
            Long expireMinutes = getLongNull(expireMinutesS);
     
            String cloudToken = null;
            NodeIO nodeIO = storageConfig.getNodeIO();
            NodeIO.AccessNode deliveryAccessNode = nodeIO.getAccessNode(archiveNode);
            try {
                tokenStatus = TokenManager.getCloudToken(nodeIO, archiveNode, asynchToken, logger);
            
            } catch (TException tex) {
                tokenGetState = TokenGetState.getTokenGetState(tex);
                int status = tokenGetState.getHttpStatus();
                String json = tokenGetState.getJsonError();
                return Response 
                    .status(status).entity(json)
                    .build();
            }
            tokenGetState = TokenGetState.getTokenGetState(tokenStatus);
            if (tokenGetState.getRunStatus() != TokenGetState.RunStatus.OK) {
                int status = tokenGetState.getHttpStatus();
                String json = tokenGetState.getJson();
                return Response 
                    .status(status).entity(json)
                    .build();
            }
            
            cloudToken = tokenStatus.getToken();
            String key = cloudToken + "/data";
            String contentType = tokenStatus.getArchiveTypeMime();
            String nodeIOName = tokenStatus.getNodeIOName();
            
            long nodeID = tokenStatus.getDeliveryNode();
            int nodeIDI = (int)nodeID;
            PreSignedState presignState = CloudUtil.getPreSignedURI(
                nodeIO,
                nodeIDI,    
                key,
                expireMinutes,
                contentType,
                contentDisposition,
                logger);
            
            if (presignState.getStatusEnum() == PreSignedState.StatusEnum.DATA_EXPIRATION) {
                tokenGetState.setRunStatus(TokenGetState.RunStatus.DATA_EXPIRATION);
                int status = tokenGetState.getHttpStatus();
                String json = tokenGetState.getJsonError();
                return Response 
                    .status(status).entity(json)
                    .build();
            }
            Exception ex = presignState.getEx();
            if (presignState.getStatusEnum() != PreSignedState.StatusEnum.OK) {
                tokenGetState.setRunStatus(TokenGetState.RunStatus.SERVICE_EXCEPTION);
                tokenGetState.setEx(ex);
                int status = tokenGetState.getHttpStatus();
                String json = tokenGetState.getJsonError();
                return Response 
                    .status(status).entity(json)
                    .build();
            }
            URL url = presignState.retrieveUrl();
            tokenGetState.setUrl(url);
            int status = tokenGetState.getHttpStatus();
            String json200 = tokenGetState.getJson();
            return Response 
                    .status(status).entity(json200)
                    .build();
            
        } catch (TException tex) {
            tokenGetState = TokenGetState.getTokenGetState(tex);
            int status = tokenGetState.getHttpStatus();
            String json = tokenGetState.getJsonError();
            return Response 
                .status(status).entity(json)
                .build();
            
        } finally {
        }
    }
    /**
     * 
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier
     * "false"=return dflat as is
     * @param returnIfErrorS "true"=no fixity test, "false"=fixity test
     * @param archiveTypeS user provided format type for created container
     * @param formatType user provided format type synchronized response
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
     public Response setProducerAsync(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String returnIfErrorS,
            String name,
            String email,
            String archiveTypeS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        int versionID = getVersionID(versionIDS);
        Identifier objectID = getObjectID(objectIDS);
        StorageServiceInf storageService = null;
        StateInf responseState = null;
        try {
            log("setProducerAsync entered:" + NL
                    + " - nodeID=" + nodeID+ NL
                    + " - returnIfErrorS=" + returnIfErrorS+ NL
                    + " - name=" + name+ NL
                    + " - email=" + email+ NL
                    + " - archiveTypeS=" + archiveTypeS
                    + " - formatType=" + formatType
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            responseState = storageService.getVersionState(nodeID, objectID, versionID);

         } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
        //validate archive type options before Jersey streaming
        archiveTypeS = archiveTypeS.toLowerCase();
        ArchiveBuilderBase.ArchiveType archiveType = null;
        try {
            if (DEBUG) System.out.println(MESSAGE + "getObjectStream before ArchiveBuilder");
            archiveType
                    = ArchiveBuilderBase.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }

        if (archiveType == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE
                    + "getVersionArchive - archive type not recognized");
        }
        
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        try {
            ProducerStreamingOutput streamingOutput = new ProducerStreamingOutput(
                nodeID,
                objectID,
                versionID,
                storageService,
                returnIfError,
                archiveTypeS);
            //Response tResponse = getFileResponseEntityTest(streamingOutput, formatType, "thisisatest.txt");
            
            Response errorResponse = setAsyncArchive(storageService, streamingOutput, name, email, formatType, logger);
            if (errorResponse != null) return errorResponse;
            return getStateResponse(responseState, formatType, logger, cs, sc);
            
        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    /**
      * Perform async get operation on archive/container file
      * @param storageService Storage servic
      * @param streamOutput Output stream object that will perform archive action
      * @param name name used by UI for content
      * @param email email profile
      * @param objectIDS object
      * @param formatType
      * @param logger
      * @return
      * @throws TException 
      */
    protected Response setAsyncArchive(
            StorageServiceInf storageService,
            StreamingOutput streamOutput,
            String name,
            String email,
            String formatType,
            LoggerInf logger)
        throws TException
    {
        try {
            if (StringUtil.isEmpty(name)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "mail name not provided");
            }
            log("getCloudManifest entered:"
                    + " - name=" + name
                    + " - email=" + email
                    + " - name=" + name
                    + " - formatType=" + formatType
                    );
            StorageConfig storageConfig = storageService.getStorageConfig();
            
            Properties storageProp = storageConfig.getAsyncArchivProp();
            if (storageProp == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "setAsyncArchive storage property not found");
            }
            String baseEmailDirS = storageProp.getProperty("baseEmailDir");
            if (StringUtil.isEmpty(baseEmailDirS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "baseEmailDir property not found");
            }
            File baseDir = new File(baseEmailDirS);
            if (!baseDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "baseEmailDir directory not found:" + baseEmailDirS);
            }
            File outFile = new File(baseDir, name);
            AsyncContainerObject asyncContainer = AsyncContainerObject.getAsyncContainerObject(
                storageProp,
                streamOutput,
                outFile,
                name,
                email,
                logger);
            Thread asyncThread = new Thread(asyncContainer);
            asyncThread.start();
            //asyncContainer.callEx();
            return null;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    
    }
    
    public Response getCloudManifest(
            int nodeID,
            String objectIDS,
            String validateS,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;

        boolean validate = setBool(validateS, false, true, false);
        try {
            Identifier objectID = getObjectID(objectIDS);
            log("getCloudManifest entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - validate=" + validate
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            FileContent content = storageService.getCloudManifest(nodeID, objectID, validate);
            FormatType formatTypeE = FormatType.xml;
            String fileResponseName = getResponseFileName(objectIDS + "--manifest", formatTypeE.getExtension());
            return getFileResponse(content, "xml", fileResponseName, cs, logger);

        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response getCloudManifestStream(
            int nodeID,
            String objectIDS,
            String validateS,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;

        boolean validate = setBool(validateS, false, true, false);
        StorageServiceInit storageServiceInit = null;
        StorageServiceInf storageService = null;
        Identifier objectID = null;
        String formatType = "xml";
        try {
            objectID = getObjectID(objectIDS);
            log("getCloudManifestStream entered:"
                    + " - formatType=" + formatType
                    + " - objectID=" + objectID.getValue()
                    + " - validate=" + validate
                    + " - nodeId=" + nodeID
                    );
            storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getObjectState(nodeID, objectID);

         } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
        //************************
        
        try {
            StreamingManifest streamingManifestOutput = new StreamingManifest(
                nodeID,
                objectID,
                storageService,
                validate);
            FormatType formatTypeE = FormatType.xml;
            String fileResponseName = getResponseFileName(objectIDS + "--manifest", formatTypeE.getExtension());
            return getFileResponseEntity(streamingManifestOutput, "xml", fileResponseName);

        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier for state information
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
    public Response getVersionArchive(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String returnIfErrorS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            log("getVersionArchive entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - returnIfError=" + returnIfError
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            FileContent content = storageService.getVersionArchive(nodeID, objectID, versionID, returnIfError, formatType);
            FormatType formatTypeE = FormatType.valueOf(formatType);
            String fileResponseName = objectIDS + "_" + versionIDS;
            fileResponseName = getResponseFileName(fileResponseName, formatTypeE.getExtension());
            return getFileResponse(content, formatType, fileResponseName, cs, logger);

        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier for state information
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
    public Response getVersionArchiveStream(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String returnIfErrorS,
            String debugResponseS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        boolean debugResponse = setBool(debugResponseS, false, false, false);
        StorageServiceInit storageServiceInit = null;
        StorageServiceInf storageService = null;
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            log("getVersionArchiveStream entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - returnIfError=" + returnIfError
                    );
            storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StateInf responseState = storageService.getVersionState(nodeID, objectID, versionID);

        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
                
        
        //validate archive type options before Jersey streaming
        String archiveTypeS = formatType.toLowerCase();
        ArchiveBuilderBase.ArchiveType archiveType = null;
        try {
            if (DEBUG) System.out.println(MESSAGE + "getObjectStream before ArchiveBuilder");
            archiveType
                    = ArchiveBuilderBase.ArchiveType.valueOf(archiveTypeS);
        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(MESSAGE + "Archive form unsupported:" + archiveTypeS);
        }
        
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            
            VersionStreamingOutput streamingOutput = new VersionStreamingOutput(
                nodeID,
                objectID,
                versionID,
                storageService,
                returnIfError,
                formatType);
            String fileResponseName = objectIDS + "_" + versionIDS;
            fileResponseName = getResponseFileName(fileResponseName, archiveType.getExtension());
            if (debugResponse) {
                return getFileResponseEntityTest(streamingOutput, formatType, fileResponseName);
            }
            return getFileResponseEntity(streamingOutput, formatType, fileResponseName);
            
            
            
            
            
        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    

    /**
     * Get archive for file for this specific Object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier for state information
     * @param formatType user provided format type
     * @param sc ServletConfig used to get system configuration
     * @throws TException processing exception
     */
    public Response getProducerVersion(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String returnIfErrorS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        boolean returnIfError = setBool(returnIfErrorS, false, true, false);
        try {
            int versionID = getVersionID(versionIDS);
            Identifier objectID = getObjectID(objectIDS);
            log("getVersionArchive entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - returnIfError=" + returnIfError
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            FileContent content = storageService.getProducerVersion(nodeID, objectID, versionID, returnIfError, formatType);
            FormatType formatTypeE = FormatType.valueOf(formatType);
            String fileResponseName = objectIDS + "_" + versionIDS;
            fileResponseName = getResponseFileName(fileResponseName, formatTypeE.getExtension());
            return getFileResponse(content, formatType, fileResponseName, cs, logger);

        } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);
            } catch (TException tex2) {
                throw tex2;
            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    
   /**
     * Get addVersion manifest for this object version
     * @param nodeID node identifier for state information
     * @param objectIDS object identifier for state information
     * @param versionIDS version identifier for state information
     * @param sc ServletConfig used to get system configuration
     * @return manifest file
     * @throws TException processing exception
     */
    public Response getVersionLink(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String presignS,
            String updateS,
            String filterS,
            String formatS,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
       LoggerInf logger = defaultLogger;
       try {
                   
            int versionID = getVersionID(versionIDS);
            Boolean presign = setBoolean(presignS);
            Boolean update = setBoolean(updateS);
            Identifier objectID = getObjectID(objectIDS);
            String formatType = "txt";
            log("getVersionLink entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            
            ContentVersionLink.Request cvlRequest = new ContentVersionLink.Request()
                    .setNodeID(nodeID)
                    .setObjectID(objectIDS)
                    .setVersionID(versionIDS)
                    .setPresign(presignS)
                    .setUpdate(updateS)
                    .setFilter(filterS)
                    .setOutputFormat(formatS);
            System.out.println(cvlRequest.dump(NAME + " - getVersionLink"));
            
            FileContent fileContent = storageService.getVersionLink(cvlRequest);
            String fileResponseName = objectIDS + "_" + versionIDS + "_manifest.txt";
            return getFileResponse(fileContent, formatType, fileResponseName, cs, logger);

       } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);

            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    public Response getIngestLink(
            int nodeID,
            String objectIDS,
            String versionIDS,
            String presignS,
            String updateS,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
       LoggerInf logger = defaultLogger;
       try {
            int versionID = getVersionID(versionIDS);
            Boolean presign = setBoolean(presignS);
            Boolean update = setBoolean(updateS);
            Identifier objectID = getObjectID(objectIDS);
            String formatType = "txt";
            log("getVersionLink entered:"
                    + " - formatType=" + formatType
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    );
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            FileContent content = storageService.getIngestLink(nodeID, objectID, versionID, presign, update);
            String fileResponseName = objectIDS + "_" + versionIDS + "_manifest.txt";
            return getFileResponse(content, formatType, fileResponseName, cs, logger);

       } catch (TException tex) {
            try {
                return getExceptionResponse(cs, tex, "xml", logger);

            } catch (Exception ex2) {
                throw new TException.GENERAL_EXCEPTION(ex2);
            }

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    /**
     * Get Response to a formatted State object
     * @param responseState State object to format
     * @param formatType user specified format type
     * @param logger system logging
     * @return Jersey Response referencing formatted State object (as File)
     * @throws TException process exceptions
     */
    protected Response getStateResponse(
            StateInf responseState,
            String formatType,
            LoggerInf logger,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        TypeFile typeFile = null;
        FormatType format = null;
        try {
            format = getFormatType(formatType, "state");

        } catch (TException tex) {
            responseState = tex;
            format = FormatType.xml;
        }

        try {
            typeFile = getStateFile(responseState, format, logger);
            jerseyCleanup.addTempFile(typeFile.file);
            cs.add(jerseyCleanup);

        } catch (TException tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType);
        }
        log("getStateResponse:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());

        return Response.ok(typeFile.file, typeFile.formatType.getMimeType()).build();
    }
    
    protected Response getHostnameServ(
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String canonicalHostname = InetAddress.getLocalHost().getCanonicalHostName();
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            JSONObject json = new JSONObject();
            json.put("hostname", hostname);
            json.put("canonicalHostname", canonicalHostname);
            json.put("hostAddress", hostAddress);
            String jsonS = json.toString(2);
            return Response 
                    .ok(StringUtil.stringToStream(jsonS, "utf8"), MediaType.APPLICATION_OCTET_STREAM)
                    .build();
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    /**
     * Return a Jersey Response object after formatting an exception
     * @param exception process exception to format
     * @param formatType format to use on exception (default xml)
     * @param logger system logger
     * @return Jerse Response referencing formatted Exception output
     * @throws TException process exceptions
     */
    protected Response getExceptionResponse(
            CloseableService cs,
            TException exception, 
            String formatType, 
            LoggerInf logger)
        throws TException
    {
        System.out.println("getExceptionResponse called");
        cs.add(jerseyCleanup);
        return getExceptionResponse(exception, formatType, logger);
    }

    /**
     * Return a Jersey Response object after formatting an exception
     * @param exception process exception to format
     * @param formatType format to use on exception (default xml)
     * @param logger system logger
     * @return Jerse Response referencing formatted Exception output
     * @throws TException process exceptions
     */
    protected Response getExceptionResponse(TException exception, String formatType, LoggerInf logger)
        throws TException
    {
        if (DEBUG)
            System.out.println("TRACE:" + exception.getTrace());
        int httpStatus = exception.getStatus().getHttpResponse();
        TypeFile typeFile = null;
        FormatType format = null;
        try {
            format = getFormatType(formatType, "state");

        } catch (TException dtex) {
            format = FormatType.xml;
        }
        try {
            typeFile = getStateFile(exception, format, logger);
            jerseyCleanup.addTempFile(typeFile.file);

        } catch (TException dtex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType + "Exception:" + dtex);
        }
        log("getStateResponse:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        String accessHeader = exception.getAccessExcp();
        return Response.ok(typeFile.file, typeFile.formatType.getMimeType()).status(httpStatus).header("Access-Excp", accessHeader).build();
    }

    /**
     * Set the Jersey Response for a returned file.
     * Note that a Content-Disposition is set to force the setting of a file name on the returned file
     * @param content File data and metadata used for the response
     * @param formatType Enumerated type of format for this file
     * @param fileName name of file to be returned
     * @param logger process logging
     * @return Jersey Response with headers and content set
     * @throws TException  process exceptions
     */
    protected Response getFileResponse(
            FileContent content,
            String formatType,
            String fileName,
            CloseableService cs,
            LoggerInf logger)
        throws TException
    {
        TypeFile typeFile = new TypeFile();
        try {
            typeFile.file = content.getFile();
            typeFile.formatType = FormatType.valueOf(formatType);
            jerseyCleanup.addTempFile(typeFile.file);
            cs.add(jerseyCleanup);

        } catch (Exception tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType);
        }
        log("formatType:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        /*
        InputStream mapStream = StringUtil.stringToStream(typeFile.formatType.getMimeMap(), "UTF-8");
        MimetypesFileTypeMap mimeMap = new MimetypesFileTypeMap(mapStream);
        String mt = mimeMap.getContentType(typeFile.file);
         */
        String encoding = typeFile.formatType.getEncoding();
        if (encoding != null) {
            return Response.ok(typeFile.file, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    //.header("Content-Encoding", encoding)
                    .build();
        } else {
            return Response.ok(typeFile.file, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    .build();
        }
    } 
    
    protected Response processFlag(
            String service,
            String flagName,
            String operation,
            String payload,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    { 
        
        System.out.println("processFlag"
                + " - service:" + service
                + " - operation:" + operation
                + " - payload:" + payload
        );
        StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
        StorageServiceInf storageService = storageServiceInit.getStorageService();
        StorageConfig storageConfig = storageService.getStorageConfig();
        LoggerInf logger = null;
        try {
            logger = storageConfig.getLogger();
            String zooConnectionString = storageConfig.getQueueService();
            String zooNodeBase = storageConfig.getQueueLockBase();
            if (StringUtil.isAllBlank(service)) {
                throw new TException.INVALID_OR_MISSING_PARM("service missing");
            }
            //String zooFlagPath = service.toLowerCase() + "/" + flagName.toLowerCase();
            String zooFlagPath = service;
            if (flagName != null) {
                zooFlagPath = service + "/" + flagName;
            }
            ZooTokenHandler handler = ZooTokenHandler.getZooTokenHandler (
                zooConnectionString, 
                zooNodeBase,
                zooFlagPath,
                storageConfig.getLogger());
            
            ZooTokenState zooState = handler.processFlag(operation, payload);
            return getStateResponse(zooState, formatType, logger, cs, sc);
            
        } catch (TException tex) {
            return getExceptionResponse(cs, tex, formatType, logger);

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    protected Response getFileResponseEntity(
            StreamingOutput outStream,
            String fileName)
        throws TException
    {
        FormatType formatType = null;
        try {
            formatType = FormatType.containsExtension(fileName);

        } catch (Exception tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this fileName:" + fileName);
        }
        return getFileResponseEntity(outStream, formatType, fileName);
    }
    
    protected Response getFileResponseEntity(
            StreamingOutput outStream,
            String formatTypeS,
            String fileName)
        throws TException
    {
        FormatType formatType = null;
        try {
            formatType = FormatType.valueOf(formatTypeS);

        } catch (Exception tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatTypeS);
        }
        return getFileResponseEntity(outStream, formatType, fileName);
    }
    
    protected Response getFileResponseEntity(
            StreamingOutput outStream,
            FormatType formatType,
            String fileName)
        throws TException
    {
        TypeFile typeFile = new TypeFile();

        if (formatType == null) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Format Type not supplied");
        }
        typeFile.formatType = formatType;
        log("formatType:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        
        String encoding = typeFile.formatType.getEncoding();
        if (encoding != null) {
            return Response.ok(outStream, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    .header("Content-Encoding", encoding)
                    .build();
        } else {
            return Response.ok(outStream, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    .build();
        }
    }
    
    protected Response getFileResponseEntityTest(
            StreamingOutput outStream,
            String formatType,
            String fileName)
        throws TException
    {
        TypeFile typeFile = new TypeFile();
        try {
            typeFile.formatType = FormatType.valueOf(formatType);

        } catch (Exception tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType);
        }
        log("formatType:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        
        String encoding = typeFile.formatType.getEncoding();
        File outFile = FileUtil.getTempFile("xxx", "xxx.txt");
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(outFile);
            outStream.write(stream);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                stream.close();
            } catch (Exception ex) { }
        }
        
        if (encoding != null) {
            return Response.ok(outFile, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    .header("Content-Encoding", encoding)
                    .build();
        } else {
            return Response.ok(outFile, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    .build();
        }
    }

    /**
     * Validate and return object identifier
     * @param parm String containing objectID
     * @return object identifier
     * @throws TException invalid object identifier format
     */
    protected Identifier getObjectID(String parm)
        throws TException
    {
        return ValidateCmdParms.validateObjectID(parm);
    }

    /**
     * Validate and return version identifier
     * @param parm String containing versionID
     * @return version identifier
     * @throws TException invalid object identifier format
     */
    protected int getVersionID(String parm)
        throws TException
    {
         return ValidateCmdParms.validateVersionID(parm);
    }

    /**
     * Validate and return version identifier
     * @param parm String containing versionID
     * @return version identifier
     * @throws TException invalid object identifier format
     */
    protected Integer getVersionIDCond(String parm)
        throws TException
    {
        if (parm == null) {
            return null;
        }
         return ValidateCmdParms.validateVersionID(parm);
    }

    /**
     * Validate and return node identifier
     * @param parm String containing nodeID
     * @return node identifier
     * @throws TException invalid node identifier
     */
    protected int getNodeID(String parm)
        throws TException
    {
        return ValidateCmdParms.validateNodeID(parm);
    }
    
    protected Long getLongNull(String longS)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(longS)) {
                return null;
            }
            long retLong = Long.parseLong(longS);
            return retLong;
            
        } catch (Exception ex) {
            return null;
        }
    }
    
    protected Integer getIntNull(String intS)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(intS)) {
                return null;
            }
            int retLong = Integer.parseInt(intS);
            return retLong;
            
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Validate and return node identifiers
     * @param parm String containing nodeIDs - comma delimited
     * @return node identifier
     * @throws TException invalid node identifier
     */
    protected int [] getNodeIDs(String parm)
        throws TException
    {
        return ValidateCmdParms.validateNodeIDs(parm);
    }

    /**
     * Get nodeID from the Storage Service
     * This requires that the Storage Service be resolved from the Servlet Configuration
     * ServletContext.
     * @param sc servlet Servlet Configuration contain
     * @return node identifier
     * @throws TException invalid NodeID
     */
    protected int getNodeID(ServletConfig sc)
        throws TException
    {
        log(MESSAGE + "getNodeID entered");
        StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInitDefault(sc);
        StorageServiceInf storageService = storageServiceInit.getStorageService();
        Integer storageID = storageService.getDefaultNodeID();
        return storageID;
    }

    /**
     * Verify and return File Name (or File identifier)
     * @param parm String containing file Name
     * @return file name
     * @throws TException invalid file name
     */
    protected String getFileID(String parm)
        throws TException
    {   
        return ValidateCmdParms.validateFileName(parm);
    }

    /**
     * build a file containg the addVersion manifest
     * The manifest is sent to this method either as a String value or as a url reference
     * @param manifestRequest (Alternate)String containing addVersion manifest
     * @param url (Alternate) URL reference to addVersion manifest
     * @param logger process logger
     * @return file containing addVersion manifest
     * @throws TException process exception
     */
    protected File getManifest(
            String manifestRequest,
            String url,
            LoggerInf logger)
        throws TException
    {

        try {
            URL remoteManifest = null;
            if (StringUtil.isNotEmpty(manifestRequest) && !manifestRequest.equals("none")) {
                File tempFile = FileUtil.getTempFile("manifest", ".txt");
                InputStream manifest = StringUtil.stringToStream(manifestRequest, "utf-8");
                FileUtil.stream2File(manifest, tempFile);
                return tempFile;
            }
            if (StringUtil.isNotEmpty(url) && !url.equals("none")) {
                try {
                        remoteManifest = new URL(url);
                    } catch (Exception ex) {
                        throw new TException.REQUEST_INVALID(
                            "Manifest URL is invalid:" + url);
                    }
                    File tempFile = FileFromUrl.getFile(remoteManifest, logger);
                    return tempFile;
            }
        throw new TException.REQUEST_INVALID(
                 "Neither manifest.txt nor url provided");

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(
                        "Unable to obtain manifest");
        }

    }
    
    /**
     * Generate a Jersey response using PreSignedState content
     * @param nodeIDS String nodeID for processing
     * @param key key value for nodeID cloud 
     * @param expireMinutesS String minutes until PreSigned URL expires
     * @param contentType ContentType header this data
     * @param contentDisp ContentDisp header this data
     * @param formatType
     * @param cs
     * @param sc
     * @return
     * @throws TException 
     */
    protected Response cloudPreSignFile(
            String nodeIDS,
            String key,
            String expireMinutesS,
            String contentType,
            String contentDisp,
            CloseableService cs,
            ServletConfig sc
    )
        throws TException
    {
        LoggerInf logger = null;
        try {
            int nodeID = getNodeID(nodeIDS);
            long expireMinutes = 0;
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StorageConfig storageConfig = storageService.getStorageConfig();
            System.out.println(storageConfig.dump("***cloudPreSignFile***"));
            //Properties storeProperties = storageService.getStoreProperties();
            //String nodeIOName = storeProperties.getProperty("nodePath");
            //if (nodeIOName == null) {
            //    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodePath property missing");
            //}
            try {
                expireMinutes = Long.parseLong(expireMinutesS);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "conversion minutes fails:" + expireMinutesS);
            }
            if (StringUtil.isAllBlank(contentType)) {
                contentType = null;
            }
            if (StringUtil.isAllBlank(contentDisp)) {
                contentDisp = null;
            }
            Date date = DateUtil.getCurrentDatePlus(expireMinutes * 60 * 1000);
            DateState expire = new DateState(date);
            PreSignedState responseState = CloudUtil.getPreSignedURI(
                storageConfig.getNodeIO(),
                nodeID,
                key,
                expireMinutes,
                contentType,
                contentDisp,
                logger);
            if (responseState.getStatusEnum() == PreSignedState.StatusEnum.OK) {
                Properties responseProp = responseState.getProperties();
                String json = JSONTools.simple(responseProp);
                return Response 
                    .ok(StringUtil.stringToStream(json, "utf8"), MediaType.APPLICATION_OCTET_STREAM)
                    .build();
                
            } else {
                Properties responseProp = responseState.getErrorProperties();
                String json = JSONTools.simple(responseProp);
                int status = responseState.getStatusEnum().getHttpResponse();
                
                return Response 
                    .status(status).entity(json)
                    .build();
            }
            //return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            tex.printStackTrace();
            PreSignedState responseState = PreSignedState.getPreSignedState();
            responseState.setStatusEnum(PreSignedState.StatusEnum.SERVICE_EXCEPTION);
            responseState.setEx(tex);
            Properties responseProp = responseState.getErrorProperties();
            String json = JSONTools.simple(responseProp);
            int status = responseState.getStatusEnum().getHttpResponse();
                return Response 
                    .status(status).entity(json)
                    .build();
        } 
    }

    protected Response cloudPreSignFileCAN(
            String nodeIDS,
            String key,
            String expireMinutesS,
            String contentType,
            String contentDisp,
            CloseableService cs,
            ServletConfig sc
    )
        throws TException
    {
        LoggerInf logger = null;
        try {
            int nodeID = getNodeID(nodeIDS);
            long expireMinutes = 0;
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(sc);
            StorageServiceInf storageService = storageServiceInit.getStorageService();
            logger = getNodeLogger(nodeID, storageService);
            StorageConfig storageConfig = storageService.getStorageConfig();
            NodeIO nodeIO = storageConfig.getNodeIO();
            try {
                expireMinutes = Long.parseLong(expireMinutesS);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "conversion minutes fails:" + expireMinutesS);
            }
            if (StringUtil.isAllBlank(contentType)) {
                contentType = null;
            }
            if (StringUtil.isAllBlank(contentDisp)) {
                contentDisp = null;
            }
            Date date = DateUtil.getCurrentDatePlus(expireMinutes * 60 * 1000);
            DateState expire = new DateState(date);
            System.out.println("+++cloudPreSignFileCAN: "
                    + " - nodeID:" + nodeID
                    + " - key:" + key
            );
            PreSignedState responseState = CloudUtil.getPreSignedURI(
                nodeIO,
                nodeID,    
                key,
                expireMinutes,
                contentType,
                contentDisp,
                logger);
            if (responseState.getStatusEnum() == PreSignedState.StatusEnum.OK) {
                Properties responseProp = responseState.getProperties();
                String json = JSONTools.simple(responseProp);
                return Response 
                    .ok(StringUtil.stringToStream(json, "utf8"), MediaType.APPLICATION_OCTET_STREAM)
                    .build();
                
            } else {
                Properties responseProp = responseState.getErrorProperties();
                String json = JSONTools.simple(responseProp);
                int status = responseState.getStatusEnum().getHttpResponse();
                return Response 
                    .status(status).entity(json)
                    .build();
            }
            //return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            tex.printStackTrace();
            PreSignedState responseState = PreSignedState.getPreSignedState();
            responseState.setStatusEnum(PreSignedState.StatusEnum.SERVICE_EXCEPTION);
            responseState.setEx(tex);
            Properties responseProp = responseState.getErrorProperties();
            String json = JSONTools.simple(responseProp);
            int status = responseState.getStatusEnum().getHttpResponse();
                return Response 
                    .status(status).entity(json)
                    .build();
        } 
    }
    /**
     * return integer if valid or exception if not
     * @param header exception header
     * @param parm String value of parm
     * @return parsed int value
     * @throws TException
     */
    protected int getNumber(String header, String parm)
        throws TException
    {
        try {
            return Integer.parseInt(parm);

        } catch (Exception ex) {
            throw new JerseyException.BAD_REQUEST(header + ": Number required, found " + parm);
        }
    }


    /**
     * Get StateInf formatter using Jersey FormatType
     * Involves mapping Jersey FormatType to FormatterInf.Format type
     * @param outputFormat  Jersey formattype
     * @param logger process logger
     * @return Formatter
     * @throws TException process exception
     */
    protected FormatterInf getFormatter(FormatType outputFormat, LoggerInf logger)
        throws TException
    {
        String formatS = null;
        try {
            formatS = outputFormat.toString();
            FormatterInf.Format formatterType = FormatterInf.Format.valueOf(formatS);
            return FormatterAbs.getFormatter(formatterType, logger);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("getFormatter: stack:" + StringUtil.stackTrace(ex));
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("State formatter type not supported:" + formatS);
        }
    }

    /**
     * Normalize name of response file
     * @param fileResponseName non-normalized name
     * @param extension applied extension to name
     * @return normalized name
     */
    protected String getResponseFileName(String fileResponseName, String extension)
        throws TException
    {
        if (StringUtil.isEmpty(fileResponseName)) return "";
        if (StringUtil.isEmpty(extension)) extension = "";
        else extension = "." + extension;
        fileResponseName = getResponseName(fileResponseName) + extension;
        log("getResponseFileName=" + fileResponseName);
        return fileResponseName;
    }

    /**
     * Normalize name of a file response file
     * @param fileResponseName non-normalized name
     * @return normalized name
     */
    protected String getFileResponseFileName(String fileResponseName)
        throws TException
    {
        if (StringUtil.isEmpty(fileResponseName)) return "";
        fileResponseName = fileResponseName.replace('/', '=');
        fileResponseName = fileResponseName.replace('\\', '=');
        return fileResponseName;
    }


    /**
     * Normalize response name
     * @param name name to normalize
     * @return normalized name
     */
    public static String getResponseName(String name)
        throws TException
    {
        return PairtreeUtil.getPairName(name);
    }

    /**
     * Return the log directory for specified node
     * @param nodeID node (e.g. CAN) to contain log
     * @param storageService Storage service
     * @return process logger within CAN
     * @throws TException logger not found
     */
    protected LoggerInf getNodeLogger(int nodeID, StorageServiceInf storageService)
        throws TException
    {
        LoggerInf nodeLogger = storageService.getLogger(nodeID);
        if (nodeLogger == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Node not found: " + nodeID);
        }
        return nodeLogger;
    }

    /**
     * return JerseyCleanup
     * @return JerseyCleanup
     */
    protected JerseyCleanup getJerseyCleanup()
    {
        return jerseyCleanup;
    }

    /**
     * Set boolean flag based on a passed string
     * @param valueS string to evaluate
     * @param ifNull if string is null return this
     * @param ifNone if string has not null and zero length return this
     * @param ifInvalid if string is not matched return this
     * @return true or false
     */
    public static boolean setBool(String valueS, boolean ifNull, boolean ifNone, boolean ifInvalid)
    {
        if (valueS == null) return ifNull;
        if (valueS.length() == 0) return ifNone;
        if (valueS.equals("true")) return true;
        if (valueS.equals("t")) return true;
        if (valueS.equals("yes")) return true;
        if (valueS.equals("false")) return false;
        if (valueS.equals("f")) return false;
        if (valueS.equals("no")) return false;
        return ifInvalid;
    }

    /**
     * Set boolean flag based on a passed string
     * @param valueS string to evaluate
     * @param ifNull if string is null return this
     * @param ifNone if string has not null and zero length return this
     * @param ifInvalid if string is not matched return this
     * @return true or false
     */
    public static Boolean setBoolean(String valueS)
    {
        if (valueS == null) return null;
        if (valueS.length() == 0) return null;
        valueS = valueS.toLowerCase();
        if (valueS.equals("true")) return true;
        if (valueS.equals("t")) return true;
        if (valueS.equals("yes")) return true;
        if (valueS.equals("false")) return false;
        if (valueS.equals("f")) return false;
        if (valueS.equals("no")) return false;
        return null;
    }

    /**
     * If debug flag on then sysout this message
     * @param msg message to sysout
     */
    protected void log(String msg)
    {
        if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
        log4j.trace("[JerseyStorage]>" + msg);
        //logger.logMessage(msg, 0, true);
    }

    /**
     * Container class for file and Jersey FormatType enum
     */
    public class TypeFile
    {
        public FormatType formatType = null;
        public File file = null;
    }
    
    public static class StreamOutputFile
        implements Runnable
    {
        private static final boolean DEBUG = false;
        protected StreamingOutput streamOutput = null;
        protected File outFile = null;
        protected Exception saveException = null;
    
        public StreamOutputFile(
                StreamingOutput streamOutput,
                File outFile)
        {
            this.streamOutput = streamOutput;
            this.outFile = outFile;
    }

        @Override
        public void run()
        {
            OutputStream outStream = null;
            try {
                outStream = new FileOutputStream(outFile);
                streamOutput.write(outStream);
            } catch (Exception ex) {
                ex.printStackTrace();
                saveException = ex;

            } finally {
                try {
                    outStream.close();
                } catch (Exception ex) { }
            }
        }

        protected void log(String msg)
        {
            if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
            //logger.logMessage(msg, 0, true);
        }
    }
    
    
    public static class StreamingManifest
        implements StreamingOutput
    {
        private static final boolean DEBUG = false;
        protected int nodeID = 0;
        protected Identifier objectID = null;
        protected StorageServiceInf storageService = null;
        protected boolean validate = false;
        protected String formatType = null;
    
        public StreamingManifest(
                int nodeID,
                Identifier objectID,
                StorageServiceInf storageService,
                boolean validate)
        {
            
                this.nodeID = nodeID;
                this.objectID = objectID;
                this.storageService = storageService;
                this.formatType = formatType;
                log("StreamingManifest entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - validate=" + validate
                    + " - formatType=" + formatType);
        }

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                if (false) throw new RuntimeException("Made it into write");
                try {
                    log("StreamingManifest write called");
                    storageService.getCloudManifestStream(
                            nodeID, objectID, validate, output);
                    
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
    
            protected void log(String msg)
            {
                if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
                //logger.logMessage(msg, 0, true);
            }
    }
    
    public static class ObjectStreamingOutput
        implements StreamingOutput
    {
        private static final boolean DEBUG = false;
        protected int nodeID = 0;
        protected Identifier objectID = null;
        protected StorageServiceInf storageService = null;
        protected boolean returnFullVersion = false;
        protected boolean returnIfError = false;
        protected String formatType = null;
    
        public ObjectStreamingOutput(
                int nodeID,
                Identifier objectID,
                StorageServiceInf storageService,
                boolean returnFullVersion,
                boolean returnIfError,
                String formatType)
        {
            
                this.nodeID = nodeID;
                this.objectID = objectID;
                this.storageService = storageService;
                this.returnFullVersion = returnFullVersion;
                this.returnIfError = returnIfError;
                this.formatType = formatType;
                log("ObjectStreamingOutput entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - returnFullVersion=" + returnFullVersion
                    + " - returnIfError=" + returnIfError
                    + " - formatType=" + formatType);
        }

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                if (false) throw new RuntimeException("Made it into write");
                try {
                    log("ObjectStreamingOutput write called");
                    storageService.getObjectArchiveStream(
                            nodeID, objectID, returnFullVersion, returnIfError, formatType, output);
                    
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
    
            protected void log(String msg)
            {
                if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
                //logger.logMessage(msg, 0, true);
                log4j.debug("[JerseyStorage]>" + msg);
            }
    }
    
    public static class VersionStreamingOutput
        implements StreamingOutput
    {
        private static final boolean DEBUG = false;
        protected int nodeID = 0;
        protected Identifier objectID = null;
        protected int versionID = -1;
        protected StorageServiceInf storageService = null;
        protected boolean returnIfError = false;
        protected String formatType = null;
    
        public VersionStreamingOutput(
                int nodeID,
                Identifier objectID,
                int versionID,
                StorageServiceInf storageService,
                boolean returnIfError,
                String formatType)
        {
            
                this.nodeID = nodeID;
                this.objectID = objectID;
                this.versionID = versionID;
                this.storageService = storageService;
                this.returnIfError = returnIfError;
                this.formatType = formatType;
                log4j.debug("VersionStreamingOutput entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - returnIfError=" + returnIfError
                    + " - formatType=" + formatType);
        }

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                if (false) throw new RuntimeException("Made it into write");
                try {
                    log("VersionStreamingOutput write called");
                    storageService.getVersionArchiveStream(
                            nodeID, objectID, versionID, returnIfError, formatType, output);
                    
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
    
            protected void log(String msg)
            {
                if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
                //logger.logMessage(msg, 0, true);
            }
    }
    
    public static class ProducerStreamingOutput
        implements StreamingOutput
    {
        private static final boolean DEBUG = false;
        protected int nodeID = 0;
        protected Identifier objectID = null;
        protected int versionID = -1;
        protected StorageServiceInf storageService = null;
        protected boolean returnIfError = false;
        protected String formatType = null;
    
        public ProducerStreamingOutput(
                int nodeID,
                Identifier objectID,
                int versionID,
                StorageServiceInf storageService,
                boolean returnIfError,
                String formatType)
        {
            
                this.nodeID = nodeID;
                this.objectID = objectID;
                this.versionID = versionID;
                this.storageService = storageService;
                this.returnIfError = returnIfError;
                this.formatType = formatType;
                log4j.debug("ProducerStreamingOutput entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - returnIfError=" + returnIfError
                    + " - formatType=" + formatType);
        }

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                if (false) throw new RuntimeException("Made it into write");
                try {
                    log("VersionStreamingOutput write called");
                    storageService.getProducerArchiveStream(
                            nodeID, objectID, versionID, returnIfError, formatType, output);
                    
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
    
            protected void log(String msg)
            {
                if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
                //logger.logMessage(msg, 0, true);
            }
    }
    
    public static class ComponentStreamingOutput
        implements StreamingOutput
    {
        private static final boolean DEBUG = false;
        protected int nodeID = 0;
        protected Identifier objectID = null;
        protected int versionID = -1;
        protected String fileName = null;
        protected StorageServiceInf storageService = null;
    
        public ComponentStreamingOutput(
                int nodeID,
                Identifier objectID,
                int versionID,
                String fileName,
                StorageServiceInf storageService)
        {
            
                this.nodeID = nodeID;
                this.objectID = objectID;
                this.versionID = versionID;
                this.fileName = fileName;
                this.storageService = storageService;
                log4j.debug("ComponentStreamingOutput entered:"
                    + " - nodeId=" + nodeID
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID);
        }

            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                if (false) throw new RuntimeException("Made it into write");
                try {
                    log("VersionStreamingOutput write called");
                    storageService.getFileStream(nodeID, objectID, versionID, fileName, outputStream);
                    
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
    
            protected void log(String msg)
            {
                if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
                //logger.logMessage(msg, 0, true);
            }
    }
    
    public static class CloudStreamingOutput
        implements StreamingOutput
    {
        private static final boolean DEBUG = false;
        protected final CloudStoreInf service;
        protected final String bucket;
        protected final String key;
    
        public CloudStreamingOutput(
                CloudStoreInf service,
                String bucket,
                String key)
        {
            
            this.service = service;
            this.bucket = bucket;
            this.key = key;
            log("CloudStreamingOutput entered:"
                + " - bucket=" + bucket
                + " - key=" + key);
        }

            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                if (false) throw new RuntimeException("Made it into write");
                InputStream inputStream = null;
                try {
                    log("CloudStreamingOutput write called");
                    
                    CloudResponse response = new CloudResponse(bucket, key);
                    inputStream = service.getObjectStreaming(bucket, key, response);
                    FileUtil.stream2Stream(inputStream, outputStream);
                    
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                    
                } finally {
                    try {
                        if (inputStream != null) inputStream.close();
                    } catch (Exception ex) { }
                }
            }
    
            protected void log(String msg)
            {
                if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
                //logger.logMessage(msg, 0, true);
            }
    }

}
