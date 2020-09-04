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

import org.cdlib.mrt.core.FileComponent;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Vector;

import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.LinkedHashList;
/**
 * Version State information
 * Note that the delta values below are used to update the CAN log/summary-stat.txt
 * table. The deltas are only set in addVersion and represent changes to previous
 * version for number of file, and total file size.
 * @author dloy
 */
public class VersionState
        implements VersionStateInf, StateInf, Serializable
{

    protected Identifier objectID = null;
    protected int versionID = -1;
    protected int numFiles = 0;
    public long size = 0;
    protected Long deltaNumFiles = null;
    protected Long deltaSize = null;
    protected Long numActualFiles = null;
    protected Long totalActualSize = null;
    boolean current = false;
    protected DateState creationDate = null;
    protected URL accessURL = null;
    protected Integer nodeID = null;
    protected Integer physicalNode = null;

    //protected VersionContent versionContent = null;
    protected Vector<String> fileNames = new Vector<String>(20);
    protected LinkedHashList<String, String> versionRef = null;

    public Vector<String> retrieveFileNames() {
        return fileNames;
    }

    public void setVersionRef(LinkedHashList<String, String> versionRef) {
        this.versionRef = versionRef;
    }

    public void setAccess(Integer nodeID, URL accessURL, Identifier objectID)
    {
        this.nodeID = nodeID;
        this.accessURL = accessURL;
        this.objectID = objectID;
    }

    public void setFileNames(ComponentContent versionContent)
    {

        List<FileComponent> fileStates = versionContent.getFileComponents();
        FileComponent fileState = null;
        if ((fileStates == null) || (fileStates.size() == 0)) return;
        for (int i=0; i < fileStates.size(); i++) {
            fileState = fileStates.get(i);
            String fileName = fileState.getIdentifier();
            fileNames.add(fileName);
        }
    }

    @Override
    public URL getObjectState() {
        if ((accessURL == null) || (objectID == null)) return null;
        return StoreUtil.buildContentURL("state", accessURL, nodeID, objectID, null, null);
    }

    @Override
    public URL getVersion() {
        if ((accessURL == null) || (objectID == null)) return null;
        return StoreUtil.buildContentURL("content", accessURL, nodeID, objectID, versionID, null);
    }

    /**
     * Get list of VersionState values
     * @return
     */
    public LinkedHashList<String, String> getFileStates()
    {
        if (accessURL == null) {
            return null;
        }
        if (fileNames.size() == 0) return null;

        if (versionRef == null) {
            versionRef = new LinkedHashList<String, String>(5);
            String fileName = null;
            for (int i=0; i < fileNames.size(); i++) {
                fileName = fileNames.get(i);
                URL url = StoreUtil.buildContentURL("state", accessURL, nodeID, objectID, versionID, fileName);
                if (url != null) versionRef.put("fileState", url.toString());
            }
        }
        if (versionRef.size() == 0) return null;
        return versionRef;
    }

    /**
     * Set object identifier
     * @param objectID object identifier
     */
    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    @Override
    public int getIdentifier() {
        return versionID;
    }

    @Override
    public DateState getCreated() {
        return creationDate;
    }

    /**
     * Set creation date-time
     * @param creationDate creation date-time
     */
    public void setCreated(DateState creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public boolean isCurrent() {
        return current;
    }

    /**
     * Set true if this version is the current version
     * @param current true=current, false=not current
     */
    public void setCurrent(boolean current) {
        this.current = current;
    }

    @Override
    public int getNumFiles() {
        return numFiles;
    }

    /**
     * Set total number of files in Version
     * @param numFiles total number of files in Version
     */
    public void setNumFiles(int numFiles) {
        this.numFiles = numFiles;
    }

    @Override
    public long getTotalSize() {
        return size;
    }

    /**
     * Set size in bytes of files in version
     * @param size size in bytes of files in version
     */
    public void setTotalSize(long size) {
        this.size = size;
    }

    /**
     * Used in AddVersion process
     * @return difference in number of files from previous
     */
    public Long getDeltaNumFiles() {
        return deltaNumFiles;
    }

    public void setDeltaNumFiles(long deltaNumFiles) {
        this.deltaNumFiles = deltaNumFiles;
    }

    public Long getDeltaSize() {
        return deltaSize;
    }

    public void setDeltaSize(long deltaSize) {
        this.deltaSize = deltaSize;
    }

    public void setIdentifier(int versionID) {
        this.versionID = versionID;
    }

    @Override
    public Long getNumActualFiles() {
        return numActualFiles;
    }

    public void setNumActualFiles(Long numActualFiles) {
        this.numActualFiles = numActualFiles;
    }

    @Override
    public Long getTotalActualSize() {
        return totalActualSize;
    }

    public void setTotalActualSize(Long totalActualSize) {
        this.totalActualSize = totalActualSize;
    }

    @Override
    public Integer getPhysicalNode() {
        return physicalNode;
    }

    public void setPhysicalNode(Integer physicalNode) {
        this.physicalNode = physicalNode;
    }

    public String dump(String header)
    {
        return header
                + " - versionID=" + versionID
                + " - numFiles=" + numFiles
                + " - size=" + size
                + " - current=" + current
                + " - creationDate=" + creationDate;
    }
}
