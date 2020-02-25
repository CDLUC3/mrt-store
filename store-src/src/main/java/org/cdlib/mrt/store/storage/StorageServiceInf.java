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
*********************************************************************/
package org.cdlib.mrt.store.storage;


import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.util.Properties;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.PingState;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.store.FileState;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.ObjectFixityState;
import org.cdlib.mrt.store.StorageServiceState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.StoreNodeManager;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * This interface defines the functional API for a Curational Storage Service
 * @author dloy
 */
public interface StorageServiceInf
{
    /**
     * Add an object to this storage service
     * @param nodeID node identifier for object
     * @param objectID object identifier
     * @param context access group/profile for this item
     * @param localID local identifier
     * @param manifestFile manifest file defining object contents
     * @return Version state for added version
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState addVersion (
            int nodeID,
            Identifier objectID,
            String context,
            String localID,
            File manifestFile)
    throws TException;
    
    /**
     * Update object using existing content
     * @param nodeID node identifier for object
     * @param objectID object identifier
     * @param context access group/profile for this item
     * @param localID local identifier
     * @param manifestFile manifest file defining object contents
     * @param deleteList list of fileIDs to be deleted by this new version
     * @return Version state for added version
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState updateVersion (
            int nodeID,
            Identifier objectID,
            String context,
            String localID,
            File manifestFile,
            String [] deleteList)
    throws TException;

    /**
     * Remove the current version from this object
     * If no versions remain then delete object
     * @param nodeID node identifier for object
     * @param objectID object identifier
     * @return Version state for added version
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState deleteVersion (
            int nodeID,
            Identifier objectID,
            int versionID)
    throws TException;

    /**
     * Remove object with all versions
     * If no versions remain then delete object
     * @param nodeID node identifier for object
     * @param objectID object identifier
     * @return Object State for deleted object
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState deleteObject (
            int nodeID,
            Identifier objectID)
    throws TException;

    /**
     * Get state information about this Storage Service
     * @return Storage Service state information
     * @throws TException Exception condition during storage service procssing
     */
    public StorageServiceState getServiceState()
        throws TException;


    /**
     * Get runtime properties for monitoring Storage
     * @param doGC do garbage collection before response
     * @return Ping state information
     * @throws TException Exception condition during storage service procssing
     */
    public PingState getPingState(boolean doGC)
        throws TException;
    
    /**
     * Get state information about a specific node
     * @param nodeID node identifier
     * @return node state information
     * @throws TException Exception condition during storage service procssing
     */
    public NodeState getNodeState (
            int nodeID)
        throws TException;
    
    /**
     * search all existing nodes for this object
     * @param objectID object identifier
     * @return node state information
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState findObjectState (
            Identifier objectID)
        throws TException;

    /**
     * Get state information about a specific Object
     * @param nodeID node identifier for object
     * @param objectID object identifier
     * @return Object state information
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState getObjectState (
            int nodeID,
            Identifier objectID)
        throws TException;

    /**
     * Get an archive file containing the contents for a specific
     * object
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param returnFullVersion return archive without delta's
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @return archive object
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getObjectArchive(
            int nodeID,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException;

    
    /**
     * Get an archive file containing the contents for a specific
     * object
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param returnFullVersion return archive without delta's
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @param outputStream web output stream
     * @throws TException Exception condition during storage service procssing
     */
    public void getObjectArchiveStream(
            int nodeID,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException;
    
    /**
     * Get manifest.xml for a cloud object
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param validate true=do manifest validation, false=skip manifest validatiion
     * @return archive object
     * @throws TException Exception condition during storage service procssing
     */
     public FileContent getCloudManifest(
             int nodeID,
             Identifier objectID,
             boolean validate)
         throws TException;
    
    /**
     * Get manifest.xml for a cloud object
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param outStream output stream for manifest.xml
     * @param validate true=do manifest validation, false=skip manifest validatiion
     * @return archive object
     * @throws TException Exception condition during storage service procssing
     */
      public void getCloudManifestStream(
            int nodeID,
            Identifier objectID,
            boolean validate,
            OutputStream outStream)
          throws TException;

    /**
     * 
     * @param nodeID virtual node only
     * @param objectID object identifier of object to be copied
     * @return Object State of copied object
     * @throws TException system exception
     */
    public ObjectState copyObject (
            int nodeID,
            Identifier objectID)
    throws TException;
    
    /**
     * Copy object from sourceNodeID to targetNodeID
     * @param sourceNodeID source Node for copy
     * @param targetNodeID target Node for copy
     * @param objectID object identifier of object to be copied
     * @return Object State of copied object
     * @throws TException system exception
     */
    public ObjectState copyObject (
            int sourceNodeID,
            int targetNodeID,
            Identifier objectID)
    throws TException;
    
    /**
     * Get manifest to all object version files
     * object
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @return File content describing manifest response
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getObjectLink(
            int nodeID,
            Identifier objectID)
        throws TException;
    /**
     * Get a sigle archive file containing the producer contents for a specific
     * object-version
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param versionID version identifier for object to be retrieved
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @return archive file
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getProducerVersion(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException;
    
    /**
     * Get a sigle archive file containing the contents for a specific
     * object-version producer content only
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param versionID version identifier for object to be retrieved
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @param outputStream web output stream
     * @throws TException Exception condition during storage service procssing
     */
    public void getProducerArchiveStream(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException;
    
    /**
     * Get a sigle archive file containing the contents for a specific
     * object-version
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param versionID version identifier for object to be retrieved
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @return archive file
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getVersionArchive(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object-version
     * @param nodeID node identifier for object to be retrieved
     * @param objectID object identifier for object to be retrieved
     * @param versionID version identifier for object to be retrieved
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @param outputStream web output stream
     * @throws TException Exception condition during storage service procssing
     */
    public void getVersionArchiveStream(
            int nodeID,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException;

    /**
     * Get the addVersion manifest for a  specific object-version
     * @param nodeID node identifier for manifest to be retrieved
     * @param objectID object identifier for manifest to be retrieved
     * @param versionID version identifier for manifest to be retrieved
     * @return addVersion manifest in file
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getVersionLink(
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException;

    /**
     * Local service method that provides object-version content that may
     * be used for testing
     * @param nodeID node identifier for object
     * @param objectID object identifier
     * @param versionID version identifier
     * @return VersionContent object
     * @throws TException Exception condition during storage service procssing
     */
    public VersionContent getVersionContent (
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException;

    /**
     * Get state information about a specific Object version
     * @param nodeID node identifier for state information
     * @param objectID object identifier for state information
     * @param versionID version identifier for state information
     *   Note that a zero or less versionID is treated as current
     * @return object-version state information
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState getVersionState (
            int nodeID,
            Identifier objectID,
            int versionID)
        throws TException;

    /**
     * Perform a fixity test on a specific node-object-version-file
     * @param nodeID node identifier
     * @param objectID object identifier
     * @param versionID version identifier
     * @param fileName file name
     * @return fixity state information from performing a fixity test
     * @throws TException Exception condition during storage service procssing
     */
    public FileFixityState getFileFixityState (
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;

    /**
     * Perform a fixity test on a specific node-object-version-file
     * @param nodeID node identifier
     * @param objectID object identifier
     * @return fixity state information from performing a fixity test
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectFixityState getObjectFixityState (
            int nodeID,
            Identifier objectID)
        throws TException;

    /**
     * Get file state information
     * @param nodeID node identifier
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @return file state information
     * @throws TException Exception condition during storage service procssing
     */
    public FileState getFileState (
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;


    /**
     * get a specific file for a node-object-version
     * @param nodeID node identifier
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @return requested file
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getFile (
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;

   
    /**
     * get a specific file for a node-object-version
     * @param nodeID node identifier
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @param outputStream stream to contain returned component
     * @throws TException Exception condition during storage service processing
     */
    public void getFileStream(
            int nodeID,
            Identifier objectID,
            int versionID,
            String fileName,
            OutputStream outputStream)
        throws TException;
   
    /**
     * get logger used for this Storage Service
     * @return file logger for service
     */
    public LoggerInf getLogger();

    /**
     * get logger for specific node
     * Storage Service logging is handled at the Node level when possible
     * @param nodeID node identifier
     * @return file logger for node
     */
    public LoggerInf getLogger(int nodeID);

    /**
     * A default nodeID is used to allow CAN functionality through the StorageService
     * A CAN service is basically a storage service defaulted to a specific nodeID
     * @return default nodeID
     */
    public Integer getDefaultNodeID();
    
    /**
     * Configuration Properties
     * @return Configuration Properties
     */
    public Properties getConfProp();

    /**
     * Storage properties from store-info.txt
     * @return Storage Properties
     */
    public Properties getStoreProperties();
}

