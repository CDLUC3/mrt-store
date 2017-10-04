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

import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.FileComponent;
import java.io.File;
import java.io.OutputStream;


import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudStoreInf;

/**
 * ObjectStore API - e.g. dflat
 * @author dloy
 */
public interface ObjectStoreInf
{
    /**
     * Copy object to this node
     * @param storeFile (currently null)
     * @param fromNode from node for object to be copied
     * @param objectID object identifier to be copied
     * @param validateWrite true=confirm output content was written
     * @return Object State
     * @throws TException 
     */
    public ObjectState copyObject (
            File storeFile,
            NodeInf fromNode,
            Identifier objectID)
    throws TException;

    /**
     * Add an object to this storage service
     * @param storeFile object directory
     * @param objectID object identifier
     * @param manifestFile manifest file defining object contents
     * @return Version state for added version
     * @throws TException Exception condition during storage service processing
     */
    public VersionState addVersion (
            File storeFile,
            Identifier objectID,
            File manifestFile)
    throws TException;
    
    /**
     * Update differs from add by the manifestFile containing only items to add or update
     * while the deleteList is used for removal of a file
     * @param storeFile object directory
     * @param objectID object identifier
     * @param manifestFile manifest file defining object contents
     * @param deleteList array of fileIDs to remove from last version to current version
     * @return Version state for added version
     * @throws TException Exception condition during storage service processing
     */
    public VersionState updateVersion (
            File storeFile,
            Identifier objectID,
            File manifestFile,
            String [] deleteList)
    throws TException;

    /**
     * Remove current version from an object
     * @param storeFile object directory
     * @param objectID object identifier
     * @return Version state for current version
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState deleteVersion (
            File storeFile,
            Identifier objectID,
            int versionID)
    throws TException;

    /**
     * Remove object
     * @param storeFile object directory
     * @param objectID object identifier
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState deleteObject (
            File storeFile,
            Identifier objectID)
    throws TException;
    
    /**
     * Get state information about a specific Object
     * @param storeFile object directory
     * @param objectID object identifier
     * @return Object state information
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectState getObjectState (
            File storeFile,
            Identifier objectID)
        throws TException;

    /**
     * Local service method that provides object-version content that may
     * be used for testing
     * @param storeFile object directory
     * @param objectID object identifier
     * @param versionID version identifier
     * @return ComponentContent object
     * @throws TException Exception condition during storage service procssing
     */
    public ComponentContent getVersionContent (
            File storeFile,
            Identifier objectID,
            int versionID)
        throws TException;

    /**
     * Get state information about a specific Object version
     * @param storeFile object directory
     * @param objectID object identifier for state information
     * @param versionID version identifier for state information
     *   Note that a zero or less versionID is treated as current
     * @return object-version state information
     * @throws TException Exception condition during storage service procssing
     */
    public VersionState getVersionState (
            File storeFile,
            Identifier objectID,
            int versionID)
        throws TException;

    /**
     * Use key to get file content
     * @param storeFile object directory
     * @param key cloud key
     * @param outFile target file for content
     */
    public void keyToFile (
            File storeFile,
            String key,
            File outFile)
        throws TException;
    
    /**
     * Get a specific file for a node-object-version
     * @param storeFile object directory
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @return requested file
     * @throws TException Exception condition during storage service procssing
     */
    public File getFile (
            File storeFile,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;
    
    /**
     * Get a specific file for a node-object-version
     * @param storeFile object directory
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @param outputStream stream to contain returned file
     * @throws TException Exception condition during storage service procssing
     */
    public void getFileStream (
            File storeFile,
            Identifier objectID,
            int versionID,
            String fileName,
            OutputStream outputStream)
        throws TException;

    /**
     * Perform a fixity test on a specific node-object-version-file
     * @param storeFile object directory
     * @param objectID object identifier
     * @param versionID version identifier
     * @param fileName file name
     * @return fixity state information from performing a fixity test
     * @throws TException Exception condition during storage service procssing
     */
    public FileFixityState getFileFixityState (
            File storeFile,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;

    /**
     * Perform a fixity test on a specific node-object
     * @param storeFile object directory
     * @param objectID object identifier
     * @return fixity state information from performing a fixity test
     * @throws TException Exception condition during storage service procssing
     */
    public ObjectFixityState getObjectFixityState (
            File storeFile,
            Identifier objectID)
        throws TException;

    /**
     * Get file state information
     * @param storeFile object directory
     * @param objectID object identifier
     * @param versionID version identifier
     *   Note that a zero or less versionID is treated as current
     * @param fileName file name
     * @return file state information
     * @throws TException Exception condition during storage service procssing
     */
    public FileComponent getFileState (
            File storeFile,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object
     * @param storeFile object directory
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
    public FileContent getObject(
            File objectStoreBase,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object
     * @param storeFile object directory
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
    public void getObjectStream(
            File objectStoreBase,
            Identifier objectID,
            boolean returnFullVersion,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException;


    /**
     * Get a sigle archive file containing the contents for a specific
     * object-version
     * @param storeFile object directory
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
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS)
        throws TException;

    /**
     * Get a sigle archive file containing the contents for a specific
     * object-version
     * @param storeFile object directory
     * @param objectID object identifier for object to be retrieved
     * @param versionID version identifier for object to be retrieved
     * @param returnIfError return even if fixity error occurs
     * @param archiveTypeS archive type:
     *  tar - tar file
     *  targz - gunzipped tar file
     *  zip - zip file
     * @param outputStream http stream output
     * @throws TException Exception condition during storage service procssing
     */
    public void getVersionArchiveStream(
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            boolean returnIfError,
            String archiveTypeS,
            OutputStream outputStream)
        throws TException;

    /**
     * Get the addVersion manifest for a  specific object-version
     * @param storeFile object directory
     * @param objectID object identifier for manifest to be retrieved
     * @param versionID version identifier for manifest to be retrieved
     * @return addVersion manifest in file
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getVersionLink(
            File objectStoreBase,
            Identifier objectID,
            int versionID,
            String linkBaseURL)
        throws TException;

    /**
     * Get the manifest for all version files of an object
     * @param objectStoreBase object directory
     * @param objectID object identifier for manifest to be retrieved
     * @return addVersion manifest in file
     * @throws TException Exception condition during storage service procssing
     */
    public FileContent getObjectLink(
            File objectStoreBase,
            Identifier objectID,
            String linkBaseURL)
        throws TException;

    /**
     * For cloud only services return object manifest.xml
     * @param objectID
     * @param validate true=do manifest validation, false=skip manifest validatiion
     * @return manifest.xml
     * @throws TException TException Exception condition during storage service procssing
     */
    public FileContent getCloudManifest(
            File objectStoreBase,
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
            File objectStoreBase,
            Identifier objectID,
            boolean validate,
            OutputStream outStream)
        throws TException;
    
    /**
     * get verifyOnRead switch
     *
     * @return true=option to do fixity on read, false=no fixity test should be performed
     */
    public boolean isVerifyOnRead();


    /**
     * get verifyOnWrite switch
     *
     * @return true=option to do fixity on write, false=no fixity test should be performed
     */
    public boolean isVerifyOnWrite();

    /**
     * Set verifyOnRead switch
     * @param verifyOnReadS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    public void setVerifyOnRead(boolean verifyOnReadS)
        throws TException;

    /**
     * Set verifyOnWrite switch
     * @param verifyOnWwriteS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    public void setVerifyOnWrite(boolean verifyOnWriteS)
        throws TException;
    
    /**
     * Return cloud service if exists
     */
    public CloudStoreInf getCloudService();
    
    /**
     * Return cloud bucket if exists
     */
    public String getCloudBucket();
}

