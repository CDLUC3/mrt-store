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

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StateInf;

/**
 * State information for Object
 * @author dloy
 */
public class ObjectState
        implements ObjectStateInf, StateInf, Serializable
{
    private static final long serialVersionUID = 31L;
    protected Identifier objectID = null;
    protected ArrayList<VersionState> versionList = new ArrayList<>(20);
    protected int numVersions = 0;
    protected int numFiles = 0;
    public long size = 0;
    protected Long statsNumFiles = null;
    public Long statsSize = null;
    protected URL accessURL = null;
    protected Integer nodeID = null;
    protected DateState lastFixity = null;
    protected DateState lastAddVersion = null;
    protected DateState lastDeleteVersion = null;
    protected DateState lastDeleteObject = null;
    protected String context = null;
    protected String localID = null;
    protected Integer physicalNode = null;

    protected ObjectState(
            Identifier objectID)
    {
        this.objectID = objectID;
    }

    /**
     * Factory: ObjectState
     * @param objectID Object identifier this object state
     * @return
     */
    public static ObjectState getObjectState(Identifier objectID)
    {
        return new ObjectState(objectID);
    }

    @Override
    public Identifier getIdentifier() {
        return objectID;
    }

    /**
     * Set object identifier
     * @param objectID object identifier
     */
    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    @Override
    public int getNumFiles() {
        return numFiles;
    }

    /**
     *  Set total number files for this object
     * @param numFiles number files
     */
    public void setNumFiles(int numFiles) {
        this.numFiles = numFiles;
    }

    @Override
    public long getSize() {
        return size;
    }

    /**
     * Set total byte size for this object
     * @param size in bytes for this object
     */
    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public int getNumVersions() {
        return numVersions;
    }

    /**
     * Set number of versions this object
     * @param numVersions number of versions
     */
    public void setNumVersions(int numVersions) {
        this.numVersions = numVersions;
    }

    @Override
    public Long getTotalActualSize() {
        return statsSize;
    }

    /**
     * Set total byte size for this object
     * @param size in bytes for this object
     */
    public void setTotalActualSize(long size) {
        this.statsSize = size;
    }

    @Override
    public Long getNumActualFiles() {
        return statsNumFiles;
    }

    /**
     * Set number of statistic files this object
     * @param statsNumFiles number of statistic files this object
     */
    public void setNumActualFiles(long statsNumFiles) {
        this.statsNumFiles = statsNumFiles;
    }

    /**
     * Add versionState to Object State List
     * @param versionState save version state
     */
    public void addVersion(VersionState versionState)
    {
        versionList.add(versionState);
    }

    @Override
    public URL getObject() {
        if ((accessURL == null) || (objectID == null)) return null;
        return StoreUtil.buildContentURL("content", accessURL, nodeID, objectID, null, null);
    }

    /**
     * Get list of VersionState values
     * @return
     */
    @Override
    public LinkedHashList<String, String> getVersionStates()
    {
        if (accessURL == null) {
            return null;
        }
        LinkedHashList<String, String> versionRef = new LinkedHashList<String, String>(5);
        for (int i=0; i < versionList.size(); i++) {
            VersionState versionState = versionList.get(i);
            int versionID = versionState.getIdentifier();
            URL url = StoreUtil.buildContentURL("state", accessURL, nodeID, objectID, versionID, null);
            versionRef.put("versionState", url.toString());
        }
        if (versionRef.size() == 0) return null;
        return versionRef;
    }
    
    @Override
    public URL getCurrentVersionState()
    {
        int currentVersion = getNumVersions();
        URL url = StoreUtil.buildContentURL("state", accessURL, nodeID, objectID, currentVersion, null);
        return url;
    }

    @Override
    public URL getNodeState()
    {
        URL url = StoreUtil.buildContentURL("state", accessURL, nodeID, null, null, null);
        return url;
    }

    public void setAccess(URL accessURL, Integer nodeID)
    {
        this.accessURL = accessURL;
        this.nodeID = nodeID;
    }


    public DateState getLastAddVersion() {
        return lastAddVersion;
    }

    /**
     * Set Date last Add version
     * @param lastAddVersion date last Add verion
     */
    public void setLastAddVersion(DateState lastAddVersion) {
        this.lastAddVersion = lastAddVersion;
    }

    public DateState getLastDeleteObject() {
        return lastDeleteObject;
    }

    /**
     * Set Date last object deletion
     * @param lastDeleteObject date last object deleted
     */
    public void setLastDeleteObject(DateState lastDeleteObject) {
        this.lastDeleteObject = lastDeleteObject;
    }

    public DateState getLastDeleteVersion() {
        return lastDeleteVersion;
    }

    /**
     * Date of last deleted version
     * @param lastDeleteVersion date of last deleted version
     */
    public void setLastDeleteVersion(DateState lastDeleteVersion) {
        this.lastDeleteVersion = lastDeleteVersion;
    }

    public DateState getLastFixity() {
        return lastFixity;
    }

    /**
     * Date of last fixity
     * @param lastFixity
     */
    public void setLastFixity(DateState lastFixity) {
        this.lastFixity = lastFixity;
    }

    @Override
    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    @Override
    public String getLocalID() {
        return localID;
    }

    public void setLocalID(String localID) {
        this.localID = localID;
    }

    @Override
    public int getNodeID() {
        return nodeID;
    }

    @Override
    public Integer getPhysicalNode() {
        return physicalNode;
    }

    public void setPhysicalNode(Integer physicalNode) {
        this.physicalNode = physicalNode;
    }

    /**
     * Dump basic Object state information
     * @param header
     * @return String dump of Object state information
     */
    public String dump(String header)
    {
        return header + ":"
                + " - size=" + getSize()
                + " - numFiles=" + getNumFiles()
                + " - numVersions=" + getNumVersions()
                ;
    }
 
}
