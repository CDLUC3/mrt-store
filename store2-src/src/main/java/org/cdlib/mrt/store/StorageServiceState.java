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
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.log.utility.Log4j2Util;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.PropertiesUtil;

/**
 *
 * @author dloy
 */
public class StorageServiceState
        implements StorageServiceStateInf, StateInf, Serializable
{
    protected static final String NAME = "StorageServiceState";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    public static final String CANSTATS = "summary-stats.txt";

    protected String serviceName = null;
    protected String serviceID = null;
    protected String serviceDescription = null;
    protected SpecScheme serviceScheme = null;
    protected LinkedHashList<String, String> nodesRef = new LinkedHashList<String, String>(5);
    protected Vector<NodeState> nodeStates = new Vector<NodeState>(10);
    protected Vector<NodeState> failNodes = new Vector<NodeState>(10);
    protected DateState accessDateTime = null;
    protected DateState creationDateTime = null;
    protected DateState updateDateTime = null;
    protected String commands = null;
    protected String accessURI = null;
    protected String supportURI = null;



    /**
     * Factory to get StorageService State
     * @param storageProp store-info.txt properties
     * @return StorageServiceState
     * @throws TException process exception
     */
    public static StorageServiceState getStorageServiceState(StorageConfig storageConfig)
        throws TException
    {
        return  new StorageServiceState(storageConfig);
    }

    /**
     * Constructor - extract local beans from properties
     * @param storageProp Properties containing store-info.txt values
     * @throws TException process exception
     */
    protected StorageServiceState(StorageConfig storageConfig)
        throws TException
    {
        if (DEBUG) System.out.println(storageConfig.dump("***" + NAME));
        //setName(storageProp.getProperty(SERVICENAME));
        //setIdentifier(storageProp.getProperty(SERVICEID));
        //setDescription(storageProp.getProperty(SERVICEDESCRIPTION));
        //setServiceScheme(storageProp.getProperty(SERVICESCHEME));
        setBaseURI(storageConfig.getBaseURI());
        setSupportURI(storageConfig.getSupportURI());
    }


    @Override
    public String getCommands() {
        return commands;
    }

    /**
     * Set available command string
     * @param commands String available commands
     */
    public void setCommands(String commands) {
        this.commands = commands;
    }

    @Override
    public DateState getAccessDateTime() {
        return accessDateTime;
    }

    /**
     * Set last update date-time for this Node.
     * Corresponds to the date-time for last update to the node counter file
     * log/summary-stats.txt
     * @param accessDateTime access date-time
     */
    public void setAccessDateTime(DateState accessDateTime) {
        this.accessDateTime = accessDateTime;
    }

    @Override
    public DateState getCreated() {
        return creationDateTime;
    }

 /**
     * Get creation date-time for this Node
     * Corresponds to the date-time of the node directory file
     * @param creationDateTime creation date-time for this Node
     */
    public void setCreated(DateState creationDateTime) {
        this.creationDateTime = creationDateTime;
    }

    /**
     * Retrieve Node state information
     * @return collection of NodeStates
     */
    public Vector<NodeState> retrieveNodeStates() {
        return nodeStates;
    }

    @Override
    public LinkedHashList<String, String> getNodeStates() {
        return nodesRef;
    }

    public void setNodesState(LinkedHashList<String, String> nodeStates)
    {
        nodesRef = nodeStates;
    }

    @Override
    public DateState getUpdateDateTime() {
        return updateDateTime;
    }

    /**
     * Set last update date-time for this Node.
     * Corresponds to the date-time for last update to the node counter file
     * log/summary-stats.txt
     * @param updateDateTime last update date-time for this Node.
     */
    public void setUpdateDateTime(DateState updateDateTime) {
        this.updateDateTime = updateDateTime;
    }

    /**
     * Add a NodeState to local node list
     * @param node NodeState to be added
     */
    public void addNodeState(NodeState node)
    {
        if (node == null) return;
        nodeStates.add(node);
        addNodeRef(node);

        accessDateTime = null;
        creationDateTime = null;
        updateDateTime = null;
    }
    
    /**
     * Add a NodeState to local node list
     * @param node NodeState to be added
     */
    public void addNodeState(NodeState node, boolean ok)
    {
        if (node == null) return;
        if (ok) {
            nodeStates.add(node);
            addNodeRef(node);
        } else {
            failNodes.add(node);
            System.out.println("***Add fail node"
            );
        }

        accessDateTime = null;
        creationDateTime = null;
        updateDateTime = null;
    }


    public void addNodeRef(NodeState node)
    {
        int nodeID = node.getIdentifier();
        URL accessURL = null;
        try {
            accessURL = new URL(accessURI);
        } catch (Exception ex) {
            return;
        }
        URL url = StoreUtil.buildContentURL("state", accessURL, nodeID, null, null, null);
        //System.out.println("!!!!StorageServiceState:" + url);
        nodesRef.put("nodeState", url.toString());
    }

    /**
     * Compare two DateStates and return most recent
     * @param a first DateState
     * @param b second DateState
     * @return most recent DateState
     */
    protected DateState maxTime(DateState a, DateState b)
    {
        if ((a == null) && (b == null)) return null;
        if ((a == null) && (b != null)) return b;
        if ((a != null) && (b == null)) return a;
        long al = a.getTimeLong();
        long bl = b.getTimeLong();
        if (al > bl) return a;
        return b;
    }

    @Override
    public String getIdentifier() {
        return serviceID;
    }

    /**
     * Set Storage Service identifier
     * @param serviceID Storage Service identifier
     */
    public void setIdentifier(String serviceID) {
        this.serviceID = serviceID;
    }

    @Override
    public String getName() {
        return serviceName;
    }

    /**
     * Set Storage Service Name
     * @param serviceName Storage Service Name
     */
    public void setName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String getDescription() {
        return serviceDescription;
    }

    /**
     * Set Storage Service Name
     * @param serviceDescription Storage Service Description
     */
    public void setDescription(String serviceDescription) {
        this.serviceDescription = serviceDescription;
    }

    @Override
    public String getBaseURI() {
        return accessURI;
    }

    /**
     * Set Access URI
     * @param accessURI Set Access URI
     */
    public void setBaseURI(String accessURI) {
        this.accessURI = accessURI;
    }

    @Override
    public String getSupportURI() {
        return supportURI;
    }

    /**
     * Set Support URI
     * @param supportURI Set Support URI
     */
    public void setSupportURI(String supportURI) {
        this.supportURI = supportURI;
    }

    /**
     * Retrieve SpecScheme for this Storage Service
     * @return SpecScheme for this Storage Service
     */
    public SpecScheme retrieveServiceScheme() {
        return this.serviceScheme;
    }

    @Override
    public String getServiceScheme() {
        if (serviceScheme == null) return null;
        return serviceScheme.getFormatSpec();
    }
    
    public Vector<NodeState> getFailNodes()
    {
        return this.failNodes;
    }
    
    public int getFailNodesCnt()
    {
        return this.failNodes.size();
    }

    /**
     * Get Implementation Version
     * @return Service Scheme
     */
    public String getVersion()
    {
        if (serviceScheme == null) return null;
        return serviceScheme.getReleaseVersion();

    }
    
    public static String getLogRootLevel()
    {
        try {
            return Log4j2Util.getRootLevel();
        } catch (Exception  ex) {
            return "Not found";
        }
    }

    /**
     * Set Storage Service SpecScheme
     * @param serviceScheme SpecScheme form for StorageService
     */
    public void setServiceScheme(SpecScheme serviceScheme) {
        this.serviceScheme = serviceScheme;
    }


    /**
     * Set Storage Service SpecScheme
     * @param serviceScheme String form of Service Scheme to save
     * @throws TException
     */
    public void setServiceScheme(String serviceScheme)
        throws TException
    {
        SpecScheme spec = new SpecScheme();
        spec.parse("store", serviceScheme);
        this.setServiceScheme(spec);
    }

    public String dump(String header)
    {
        return header + ":"
                    + " - serviceName=" + getName()
                    + " - serviceID=" + getIdentifier()
                    + " - serviceVersion=" + getServiceScheme()
                    + " - accessDateTime=" + getAccessDateTime()
                    + " - creationDateTime=" + getCreated()
                    + " - updateDateTime=" + getUpdateDateTime()
                    + " - creationDateTime=" + getCreated()
                    ;
    }

}

