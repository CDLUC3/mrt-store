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
package org.cdlib.mrt.store.can;

import java.io.File;
import java.util.Properties;


import org.cdlib.mrt.store.je.LocalIDDatabase;
import org.cdlib.mrt.store.NodeState;


/**
 * Base for CAN location and store base files
 * @author dloy
 */
public class NodeForm {

    protected File m_canHome = null;

    protected File m_canHomeStore = null;

    protected String m_objectStoreType = null;

    protected String m_objectLocationType = null;

    protected NodeState nodeState = null;

    protected int m_canHomeID = -1;

    protected LocalIDDatabase localIDDb = null;

    public LocalIDDatabase getLocalIDDb() {
        return localIDDb;
    }

    public void setLocalIDDb(LocalIDDatabase localIDDb) {
        this.localIDDb = localIDDb;
    }

    /**
     * Get static node state
     * @return static node state
     */
    public NodeState getNodeState() {
        return nodeState;
    }

    /**
     * Set static node state
     * @param nodeState constructor static NodeState
     */
    public void setNodeState(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    /**
     * String name of object location form (e.g. "pairtree")
     * @return Object locator type
     */
    public String getObjectLocationType() {
        return m_objectLocationType;
    }

    /**
     * Set objcet location form (e.g. "pairtree")
     * @param objectLocationType
     */
    public void setObjectLocationType(String objectLocationType) {
        this.m_objectLocationType = objectLocationType;
    }

    /**
     * Get leaf node handler type (e.g. "dflat")
     * @return leaf node handler type
     */
    public String getObjectStoreType() {
        return m_objectStoreType;
    }

    /**
     * set the leaf node handler type (e.g. "dflat");
     * @param objectStoreType leaf node handler type
     */
    public void setObjectStoreType(String objectStoreType) {
        this.m_objectStoreType = objectStoreType;
    }

    /**
     * Set the nodeID for the local CAN
     * @param nodeID of local CAN
     */
    public NodeForm () { }

    /**
     * get the local nodeID
     * @return local nodeID
     */
    public int getNodeID () {
        return m_canHomeID;
    }

    /**
     * set the local nodeID
     * @param nodeID use this nodeID
     */
    public void setNodeID (int nodeID) {
        m_canHomeID = nodeID;
    }


    /**
     * get base File directory node for this CAN
     * @return base directory node
     */
    public File getCanHome() {
        return m_canHome;
    }

    /**
     * Set the base File directory node for this CAN
     * @param canHome base directory node
     */
    public void setCanHome(File canHome) {
        this.m_canHome = canHome;
    }

    /**
     * Get the "store" directory node for this CAN
     * @return "store" directory node
     */
    public File getCanHomeStore() {
        return m_canHomeStore;
    }

    /**
     * Set the "store" directory node for this CAN
     * @param canHomeStore "store" directory node
     */
    public void setCanHomeStore(File canHomeStore) {
        this.m_canHomeStore = canHomeStore;
    }

    /**
     * dump info on this NodeForm
     * @param header display header
     * @return dump info for this NodeForm
     */
    public String dump(String header)
    {
        try {
            return header + toString();
        } catch (Exception ex) {
            return "NodeFiles.dump Exception:" + ex;
        }

    }

    /**
     * Get String containing the content of this NodeForm
     * @return NodeForm info
     */
    @Override
    public String toString()
    {
        try {
            StringBuffer buf = new StringBuffer(100);
            buf.append(" - nodeID=" + getNodeID() + "\n");
            if (m_canHome != null)
                buf.append(" - canHome=" + m_canHome.getCanonicalPath() + "\n");
            if (m_canHomeStore != null)
                buf.append(" - canHomeStore=" + m_canHomeStore.getCanonicalPath() + "\n");
            if (m_objectStoreType != null)
                buf.append(" - objectStoreType=" + m_objectStoreType + "\n");
            if (m_objectLocationType != null)
                buf.append(" - objectLocationType=" + m_objectLocationType + "\n");
            return buf.toString();
        } catch (Exception ex) {
            return "NodeFiles.toString Exception:" + ex;
        }
    }


}

