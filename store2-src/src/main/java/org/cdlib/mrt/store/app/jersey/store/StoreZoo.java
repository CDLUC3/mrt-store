/*
Copyright (c) 2005-2012, Regents of the University of California
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



import java.io.File;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.store.StorageServiceState;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.store.app.StorageServiceInit;
import org.cdlib.mrt.store.logging.LogEntryVersion;
import org.cdlib.mrt.store.storage.StorageServiceInf;
import org.cdlib.mrt.store.tools.FileFromUrl;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.glassfish.jersey.server.CloseableService;
import org.cdlib.mrt.store.app.jersey.JerseyBase;
import static org.cdlib.mrt.store.app.jersey.JerseyBase.getManifest;
import static org.cdlib.mrt.store.app.jersey.JerseyBase.getNodeLogger;
import static org.cdlib.mrt.store.app.jersey.JerseyBase.getObjectID;
import static org.cdlib.mrt.store.app.jersey.JerseyBase.validateManifest;

/**
 * Thin Jersey layer for inv handling
 * @author  David Loy
 */

public class StoreZoo extends HttpServlet
{

    protected static final String NAME = "StoreZoo";
    protected static final String MESSAGE = NAME + ": ";

    private static final Logger log4j = LogManager.getLogger();
    private StorageServiceState responseState = null;
    private StorageServiceInf storageService = null;
    
    public static StoreZoo getStoreZoo(ServletConfig servletConfig)
            throws ServletException
    {
        StoreZoo startZoo = new StoreZoo(servletConfig);
        return startZoo;
    }
    private StoreZoo (ServletConfig servletConfig)
            throws ServletException 
    
    {
        try {
            super.init(servletConfig);
            log4j.info("StartStire entered:");
            StorageServiceInit storageServiceInit = StorageServiceInit.getStorageServiceInit(servletConfig);
            storageService = storageServiceInit.getStorageService();

            responseState = storageService.getServiceState();
            
        } catch (ServletException se) {
            se.printStackTrace();
            throw se;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new RuntimeException("BYE");
        }
    }

    public StorageServiceInf getStorageService() {
        return storageService;
    }

    /**
     * Update an object to this storage service
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param manifestRequest manifest as String
     * @param url link to manifest
     * @param sizeS size of manifest
     * @param type manifest digest type (checksumtype)
     * @param value manifest digest value (checksum)uration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public VersionState updateVersionZoo(
            int nodeID,
            String objectIDS,
            String manifestRequest,
            String delete,
            String url,
            String sizeS,
            String digestType, // use null default if not available
            String digestValue)
        throws TException
    {
        //LoggerInf logger = defaultLogger;
        File manifest = null;
        try {
            log4j.debug("updateVersion entered:"
                    + " - nodeId=" + nodeID
                    + " - objectIDS=" + objectIDS
                    + " - manifest=" + manifestRequest
                    + " - url=" + url
                    + " - sizeS=" + sizeS
                    + " - digestType=" + digestType
                    + " - digestValue=" + digestValue
                    );

            sizeS = StringUtil.normParm(sizeS);
            digestType = StringUtil.normParm(digestType);
            digestValue = StringUtil.normParm(digestValue);

            Identifier objectID = JerseyBase.getObjectID(objectIDS);
            LoggerInf logger = JerseyBase.getNodeLogger(nodeID, storageService);
            
            if ((manifestRequest != null) || (url != null)) {
                manifest = JerseyBase.getManifest(manifestRequest, url, logger);
                JerseyBase.validateManifest(manifest, sizeS, digestType, digestValue);
                //jerseyCleanup.addTempFile(manifest);
            }
            String [] deleteList = null;
            if (StringUtil.isEmpty(delete)) deleteList = null;
            else {
                delete = delete.trim();
                deleteList = delete.split("\\r?\\n");
            }
            
            long startTime = System.currentTimeMillis();
            VersionState responseState = storageService.updateVersion(nodeID, objectID, null, null, manifest, deleteList);
            long nodeL = nodeID;
            LogEntryVersion logEntry = LogEntryVersion.getLogEntryVersion("update", "StoreUpdate",
                    nodeL,
                    System.currentTimeMillis() - startTime, 
                    storageService.getAwsVersion(),
                    responseState);
            logEntry.addEntry();
            return responseState;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
            
        } finally {
            FileFromUrl.delete(manifest);
        }
    }

    /**
     * Add an object to this storage service
     * @param nodeID nodeID node identifier for object
     * @param objectIDS object identifier
     * @param manifestRequest manifest as String
     * @param url link to manifest
     * @param sizeS size of manifest
     * @param type manifest digest type (checksumtype)
     * @param value manifest digest value (checksum)
     * @return VersionState
     * @throws TException processing exception
     */
    public VersionState addVersion(
            int nodeID,
            String objectIDS,
            String manifestRequest,
            String url,
            String sizeS,
            String digestType, // use null default if not available
            String digestValue)  // use null default if not available
        throws TException
    {
        File manifest = null;
        
        try {
            log4j.debug("addVersion entered:"
                    + " - nodeId=" + nodeID
                    + " - objectIDS=" + objectIDS
                    + " - manifest=" + manifestRequest
                    + " - url=" + url
                    + " - sizeS=" + sizeS
                    + " - digestType=" + digestType
                    + " - digestValue=" + digestValue
                    );

            sizeS = StringUtil.normParm(sizeS);
            digestType = StringUtil.normParm(digestType);
            digestValue = StringUtil.normParm(digestValue);

            Identifier objectID = getObjectID(objectIDS);
            LoggerInf logger = getNodeLogger(nodeID, storageService);
            manifest = getManifest(manifestRequest, url, logger);
            validateManifest(manifest, sizeS, digestType, digestValue);
            
            //jerseyCleanup.addTempFile(manifest);
            long startTime = System.currentTimeMillis();
            VersionState responseState = storageService.addVersion(nodeID, objectID, null, null, manifest);
            long nodeL = nodeID;
            LogEntryVersion logEntry = LogEntryVersion.getLogEntryVersion("add", "StoreAdd",
                    nodeL,
                    System.currentTimeMillis() - startTime,
                    storageService.getAwsVersion(),
                    responseState);
            logEntry.addEntry();
            return responseState;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
            
        } finally {
            FileFromUrl.delete(manifest);
        }
    }
}
