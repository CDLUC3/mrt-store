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
package org.cdlib.mrt.store;

import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.*;
import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;

import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.core.Identifier;

/**
 *
 * @author dloy
 */
public interface NodeInf
{
    /**
     * Get state information about a specific node
     * @return node state information
     * @throws TException Exception condition during storage service procssing
     */
    public NodeState getNodeState()
        throws TException;

    /**
     * Add an object to this storage service
     * @param objectID object identifier
     * @param context access group/profile for this item
     * @param localID local identifier
     * @param manifestFile manifest file defining object contents
     * @return
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState addVersion (
            Identifier objectID,
            String context,
            String localID,
            File manifestFile)
    throws TException;
    
    /**
     * Update an object using existing content
     * @param objectID object identifier
     * @param context access group/profile for this item
     * @param localID local identifier
     * @param manifestFile manifest file defining object contents
     * @param deleteList list of fileIDs to be deleted
     * @return
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState updateVersion (
            Identifier objectID,
            String context,
            String localID,
            File manifestFile,
            String [] deleteList)
    throws TException;

    /**
     * Remove the current version from an object
     * @param objectID object identifier
     * @return Version State of current version
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState deleteVersion (
            Identifier objectID,
            int versionID)
    throws TException;

    /**
     * Remove object with all versions
     * @param objectID object identifier
     * @return Object State of deleted object
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState deleteObject (
            Identifier objectID)
    throws TException;

    /**
     * Get state information about a specific Object
     * @param objectID object identifier
     * @return Object state information
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState getObjectState (
            Identifier objectID)
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
            Identifier objectID,
            int versionID)
        throws TException;

    /**
     * Get file state information
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @return file state information
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getFile (
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;
    
   /**
     * get a specific file for a node-object-version
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @param outputStream stream to contain returned component
     * @throws TException Exception condition during storage service processing
     */
    public void getFileStream(
            Identifier objectID,
            int versionID,
            String fileName,
            OutputStream outputStream)
        throws TException;
    
    /**
     * Perform a fixity test on a specific node-object-version-file
     * @param objectID object identifier
     * @param versionID version identifier
     * @param fileName file name
     * @return fixity state information from performing a fixity test
     * @throws TException Exception condition during storage service procssing
     */
    public FileFixityState getFileFixityState (
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;

    /**
     * Get file state information
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @return file state information
     * @throws TException Exception condition during storage service procssing
     */
    public FileComponent getFileState (
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object-version
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
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException;
      
      /**
       * Get Synchronous archive containing producer content
       * @param objectID object identifier for object to be retrieved
       * @param versionID version identifier for object to be retrieved
       * @param producerFilter
       * @param returnIfError
       * @param archiveTypeS archive type:
       *  tar - tar file
       *  targz - gunzipped tar file
       *  zip - zip file
       * @return archive containing producer info
       * @throws TException 
       */
        public FileContent getProducerVersion(
            Identifier objectID,
            int versionID, 
            boolean returnIfError,
            String archiveTypeS)
        throws TException;
        
      
      /**
       * Get Synchronous archive containing producer content
       * @param objectID object identifier for object to be retrieved
       * @param versionID version identifier for object to be retrieved
       * @param producerFilter
       * @param returnIfError
       * @param archiveTypeS archive type:
       *  tar - tar file
       *  targz - gunzipped tar file
       *  zip - zip file
       * @param OutputStream outputStream for archive

       * @throws TException 
       */
        public void getProducerVersionStream(
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object-version
     * @param objectID object identifier for object to be retrieved
     * @param versionID version identifier for object to be retrieved
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @param outputStream http stream
     * @throws TException Exception condition during storage service procssing
     */
      public void getVersionArchiveStream(
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
            Identifier objectID,
            int versionID,
            String linkBaseURL)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object
     * @param objectID object identifier for object to be retrieved
     * @param returnFullVersion return archive without delta's
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @return archive file
     * @throws TException Exception condition during storage service procssing
     */
      public FileContent getObjectArchive(
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object
     * @param objectID object identifier for object to be retrieved
     * @param returnFullVersion return archive without delta's
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @param outputStream output stream for response
     * @throws TException Exception condition during storage service procssing
     */
      public void getObjectStream(
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException;


    /**
     * Get manifest.xml for a cloud object
     * @param objectID object identifier for object to be retrieved
     * @param validate true=do manifest validation, false=skip manifest validatiion
     * @return archive object
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getCloudManifest(
            Identifier objectID,
            boolean validate)
        throws TException;

    /**
     * Get manifest.xml for a cloud object
     * @param objectID object identifier for object to be retrieved
     * @param validate true=do manifest validation, false=skip manifest validatiion
     * @param outputStream output stream for response
     * @return archive object
     * @throws TException Exception condition during storage service procssing
     */
    public void getCloudManifestStream(
            Identifier objectID,
            boolean validate,
            OutputStream outStream)
        throws TException;

    /**
     * Copy an object from one node to another - only allowed in virtual Node
     * @param objectID object identifier for manifest to be retrieved
     * @return ObjectState of copied node
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState copyObject (
            Identifier objectID)
    throws TException;
    
    /**
     * Copy an object from one node to another
     * @param objectID object identifier for manifest to be retrieved
     * @param targetNode node receiving copied content
     * @return ObjectState of copied node
     * @throws TException Exception condition during storage service processing
     */
    public ObjectState copyObject(String storageBaseURI, Identifier objectID, NodeInf targetNode)
        throws TException;
    
    /**
     * Get the manifest for all version files for an object
     * @param nodeID node identifier for manifest to be retrieved
     * @param objectID object identifier for manifest to be retrieved
     * @return addVersion manifest in file
     * @throws TException Exception condition during storage service procssing
     */
      public FileContent getObjectLink(
            Identifier objectID,
            String linkBaseURL)
        throws TException;

    /**
     * get logger used for this CAN
     * @return file logger for service
     */
    public LoggerInf getLogger ();

    /**
     * get the node identifier
     * @return node identifier
     */
    public int getNodeID();
}

