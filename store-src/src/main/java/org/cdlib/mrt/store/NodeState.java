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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.core.DateState;
import static org.cdlib.mrt.store.NodeStateInf.CSOURCENODE;
import org.cdlib.mrt.store.StorageTypesEnum;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.store.can.CANStats;

/**
 * Contains the NodeState static and dynamic data
 * Static consists of the node scheme values for node handling
 * Dynamic consists of the count information for size, objects, versions and files
 * @author dloy
 */
public class NodeState
        implements NodeStateInf, StateInf, Serializable
{
    private static final long serialVersionUID = 31L;
    protected static final String NAME = "NodeState";
    protected static final String MESSAGE = NAME + ": ";
    public static final String CANSTATS = "summary-stats.txt";
    protected static final boolean DEBUG = false;

    protected String nodeName = null;
    protected int nodeID = -1;
    protected SpecScheme branchScheme = null;
    protected SpecScheme classScheme = null;
    protected SpecScheme leafScheme= null;
    protected SpecScheme nodeType = null;
    protected StorageTypesEnum storageAccessMode;
    protected StorageTypesEnum storageAccessProtocol;
    protected StorageTypesEnum storageExternalProvider;
    protected StorageTypesEnum storageMediaConnectivity;
    protected StorageTypesEnum storageMediaType;
    protected StorageTypesEnum nodeForm;
    protected StorageTypesEnum nodeProtocol;
    protected DateState accessDateTime = null;
    protected DateState creationDateTime = null;
    protected DateState lastModified = null;
    protected DateState lastFixity = null;
    protected DateState lastAddVersion = null;
    protected DateState lastDeleteVersion = null;
    protected DateState lastDeleteObject = null;
    protected String description = null;
    protected boolean verifyOnRead = true;
    protected boolean verifyOnWrite = true;
    protected long numFiles = 0;
    protected long numVersions = 0;
    protected long numObjects = 0;
    protected long totalSize = 0;
    protected long numActualFiles = 0;
    protected long totalActualSize = 0;
    protected String baseURI = null;
    protected String supportURI = null;
    protected String logicalVolume = null;
    protected Integer targetNodeID = null;
    protected Integer sourceNodeID = null;
    protected Properties nodeStateProp = null;
    
    /**
     * Empty constructor
     */
    public NodeState() { }

    /**
     * Constructor - static
     * Set at initialization of Node
     * @param nodeID node identifier
     * @param prop NodeState properties
     * @param nodeHome File pointer to nodeHome
     * @throws TException
     */
    public NodeState(
            Properties prop,
            File nodeHome)
            throws TException
    {
        if (DEBUG) {
            System.out.println("!!!!" + MESSAGE + PropertiesUtil.dumpProperties("NodeState", prop, 50));
        }
        this.nodeStateProp = prop;
        setNodeName(prop.getProperty(CNAME, ""));
        setBranchScheme(prop);
        setClassScheme(prop);
        setLeafScheme(prop);
        setStorageAccessMode(prop.getProperty(CACCESSMODE, ""));
        setStorageAccessProtocol(prop.getProperty(CACCESSPROTOCOL, ""));
        setNodeProtocol(prop.getProperty(CNODEPROTOCOL, ""));
        setStorageMediaConnectivity(prop.getProperty(CMEDIACONNECTIVITY, ""));
        setExternalProvider(prop.getProperty(CEXTERNALPROVIDER, ""));
        setMediaType(prop.getProperty(CMEDIATYPE, ""));
        setNodeScheme(prop);
        validateNodeState(prop);
        setNodeID(prop.getProperty(CIDENTIFIER));
        setFileExtraction(nodeHome);
        setDescription(prop.getProperty(CDESCRIPTION));
        setVerifyOnRead(prop.getProperty(CVERIFYONREAD));
        setVerifyOnWrite(prop.getProperty(CVERIFYONWRITE));
        setBaseURI(prop.getProperty(CBASEURI));
        setSupportURI(prop.getProperty(CSUPPORTURI));
        setLogicalVolume(prop.getProperty(CLOGICALVOLUME));
        setNodeForm(prop.getProperty(CNODEFORM));
        setSourceNodeID(prop.getProperty(CSOURCENODE));
        setTargetNodeID(prop.getProperty(CTARGETNODE));
        //setProducerFilter(prop);
        if (DEBUG) {
            System.out.println("!!!!Node(" + nodeID + "):" + dump("NodeState constructor"));
            try {
            System.out.println("!!!!Address:" + nodeHome.getCanonicalPath());
            } catch (Exception ex) {}
        }
    }

    /**
     * Constructor - dynamic NodeState
     * Contains current counter values
     * @param nodeState
     * @param nodeHome
     * @throws TException
     */
    public NodeState(
            NodeState nodeState,
            File nodeHome)
        throws TException
    {
        this.nodeName = nodeState.nodeName;
        this.nodeID = nodeState.nodeID;
        this.branchScheme = nodeState.branchScheme;
        this.classScheme = nodeState.classScheme;
        this.leafScheme = nodeState.leafScheme;
        this.nodeType = nodeState.nodeType;
        this.nodeProtocol = nodeState.nodeProtocol;
        this.description = nodeState.description;
        this.storageAccessMode = nodeState.storageAccessMode;
        this.storageAccessProtocol = nodeState.storageAccessProtocol;
        this.storageExternalProvider = nodeState.storageExternalProvider;
        this.storageMediaType = nodeState.storageMediaType;
        this.storageMediaConnectivity = nodeState.storageMediaConnectivity;
        this.baseURI = nodeState.baseURI;
        this.supportURI = nodeState.supportURI;
        this.logicalVolume = nodeState.logicalVolume;
        this.nodeForm = nodeState.nodeForm;
        this.sourceNodeID = nodeState.sourceNodeID;
        this.targetNodeID = nodeState.targetNodeID;
        setFileExtraction(nodeHome);
        /*
        System.out.println("Node(" + nodeID + "):" + dump("cmdtest"));
        try {
        System.out.println("Address:" + nodeHome.getCanonicalPath());
        } catch (Exception ex) {}
         */
    }

    /**
     * Extract counter information from log/summary-stats.txt file and
     * reset values in this object
     * @param nodeHome
     * @throws TException
     */
    protected void setFileExtraction(File nodeHome)
            throws TException
    {
        if ((nodeHome == null) || !nodeHome.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM (
                    MESSAGE + "NodeHome file missing");
        }
        File log = new File(nodeHome, "log");
        if ((log == null) && !log.exists()) {
            log.mkdir();
        }
        File statsFile = new File(log, CANSTATS);
        CANStats stats = new CANStats(statsFile);
        numObjects = stats.getCount(CANStats.OBJECTCNT);
        numVersions = stats.getCount(CANStats.VERSIONCNT);
        numFiles = stats.getCount(CANStats.COMPONENTFILECNT);
        totalSize = stats.getCount(CANStats.COMPONENTTOTALSIZE);
        numActualFiles = stats.getCount(CANStats.FILECNT);
        totalActualSize = stats.getCount(CANStats.TOTALSIZE);

        lastModified = new DateState(statsFile.lastModified());
        creationDateTime = new DateState(nodeHome.lastModified());
    }


    /**
     * Validate that critical NodeState value are set
     * @param prop validate NodeState properties value
     * @throws TException process exception
     */
    protected void validateNodeState(Properties prop)
            throws TException
    {

        if (StringUtil.isEmpty(nodeName)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "NodeName missing - Properties:" + PropertiesUtil.dumpProperties("", prop, 50));
        }
        if (branchScheme == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "BranchScheme missing - Properties:" + PropertiesUtil.dumpProperties("", prop, 50));
        }
        if (leafScheme == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "LeafScheme missing - Properties:" + PropertiesUtil.dumpProperties("", prop, 50));
        }
    }

    @Override
    public String getAccessMode() {
        if (storageAccessMode == null) return null;
        return storageAccessMode.getName();
    }

    /**
     * Set StorageAccess Type
     * @param name Storage Access type
     * @throws TException invalid storage access value
     */
    public void setStorageAccessMode(String name)
            throws TException
    {
        this.storageAccessMode = StorageTypesEnum.valueOf("access", name);
    }

    @Override
    public String getNodeForm() {
        if (nodeForm == null) return null;
        return nodeForm.getName();
    }

    /**
     * Set StorageAccess Type
     * @param name Storage Access type
     * @throws TException invalid storage access value
     */
    public void setNodeForm(String name)
            throws TException
    {
        this.nodeForm = StorageTypesEnum.valueOf("nodeform", name);
        if (this.nodeForm == null) {
            nodeForm = StorageTypesEnum.valueOf("nodeform", "physical");
        }
    }

    @Override
    public String getNodeProtocol() {
        if (nodeProtocol == null) return null;
        return nodeProtocol.getName();
    }

    /**
     * Set StorageAccess Type
     * @param name Storage Access type
     * @throws TException invalid storage access value
     */
    public void setNodeProtocol(String name)
            throws TException
    {
        this.nodeProtocol = StorageTypesEnum.valueOf("nodeprotocol", name);
        if (this.nodeProtocol == null) {
            nodeProtocol = StorageTypesEnum.valueOf("nodeprotocol", "file");
        }
    }

    @Override
    public String getAccessProtocol() {
        if (storageAccessProtocol == null) return null;
        return storageAccessProtocol.getName();
    }

    /**
     * Set StorageProtocol Type
     * @param name Storage Protocol type
     * @throws TException invalid storage access value
     */
    public void setStorageAccessProtocol(String name)
            throws TException
    {
        if (DEBUG) System.out.println("!!!!setStorageAccessProtocol name=" + name);
        this.storageAccessProtocol = StorageTypesEnum.valueOf("protocol", name);
    }

    @Override
    public String getMediaConnectivity() {
        if (storageMediaConnectivity == null) return null;
        return storageMediaConnectivity.getName();
    }

    /**
     * Set StorageProtocol Type
     * @param name Storage Protocol type
     * @throws TException invalid storage access value
     */
    public void setStorageMediaConnectivity(String name)
            throws TException
    {
        this.storageMediaConnectivity = StorageTypesEnum.valueOf("connectivity", name);
    }

    @Override
    public String getExternalProvider() {
        if (storageExternalProvider == null) return null;
        return storageExternalProvider.getName();
    }

    /**
     * Set Storage External Provider Type
     * @param name External Provide type
     * @throws TException invalid storage access value
     */
    public void setExternalProvider(String name)
            throws TException
    {
        if (StringUtil.isNotEmpty(name)) name = name.toLowerCase();
        this.storageExternalProvider = StorageTypesEnum.valueOf("provider", name);
    }

    @Override
    public String getMediaType() {

        if (storageMediaType == null) return null;
        return storageMediaType.getName();
    }

    /**
     * Set Storage media type
     * @param name storage media type
     */
    public void setMediaType(String name) {
        this.storageMediaType = StorageTypesEnum.valueOf("media", name);
    }

    /**
     * Get local NodeState property from log/summary-stats.txt
     * @param prop NodeState property
     * @param key property key
     * @return
     */
    protected String getProp(Properties prop, String key)
    {
        return prop.getProperty(key);
    }

    @Override
    public String getBranchScheme() {
        return branchScheme.getFormatSpec();
    }

    /**
     * Set Branch Scheme from Properties
     * @param prop node Properties
     * @throws TException
     */
    public void setBranchScheme(Properties prop)
            throws TException
    {
        String line = getProp(prop, CBRANCHSCHEME);
        this.branchScheme = SpecScheme.buildSpecScheme("branch", line);
    }

    @Override
    public String getNodeScheme() {
        return nodeType.getFormatSpec();
    }

    @Override
    public String getVersion()
    {
        if (nodeType == null) return null;
        return nodeType.getReleaseVersion();

    }

    /**
     * Get Node Scheme
     * @return Leaf Scheme
     */
    public SpecScheme retrieveNodeType()
    {
        return nodeType;
    }

    /**
     * Set Node Type from Properties
     * @param prop node Properties
     * @throws TException process exception
     */
    public void setNodeScheme(Properties prop)
            throws TException
    {
        String line = getProp(prop, CNODESCHEME);
        this.nodeType = SpecScheme.buildSpecScheme("node", line);
    }

    @Override
    public String getClassScheme()
    {
        return classScheme.getFormatSpec();
    }

    /**
     * Set Class Scheme from Properties
     * @param prop node Properties
     * @throws TException process exception
     */
    public void setClassScheme(Properties prop)
            throws TException
    {
        String line = getProp(prop, CCLASSSCHEME);
        this.classScheme = SpecScheme.buildSpecScheme("class", line);
    }

    @Override
    public String getLeafScheme()
    {
        return leafScheme.getFormatSpec();
    }

    /**
     * Get Leaf Scheme
     * @return Leaf Scheme
     */
    public SpecScheme retrieveLeafScheme()
    {
        return leafScheme;
    }

    /**
     * Set Leaf Scheme from Properties
     * @param prop node Properties
     * @throws TException process Exception
     */
    public void setLeafScheme(Properties prop)
            throws TException
    {
        String line = getProp(prop, CLEAFSCHEME);
        this.leafScheme = SpecScheme.buildSpecScheme("leaf", line);
    }

    @Override
    public String getName()
    {
        return nodeName;
    }

    /**
     * Set Node Name
     * @param nodeName Node Name
     */
    public void setNodeName(String nodeName)
    {
        this.nodeName = nodeName;
    }

    @Override
    public int getIdentifier()
    {
        return nodeID;
    }

    /**
     * Set nodeID
     * @param nodeID nodeID value
     */
    public void setNodeID(int nodeID)
    {
        this.nodeID = nodeID;
    }

    /**
     * Set nodeID
     * @param nodeID nodeID value
     */
    public void setNodeID(String nodeIDS)
        throws TException
    {
        int localInt = 0;
        nodeIDS = nodeIDS.trim();
        try {
            localInt = Integer.parseInt(nodeIDS);
        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE(MESSAGE + "setNodeID: can-info.txt identifier not valid"
                    + " - nodeID=" + nodeID
                    + " - info nodeID=" + nodeIDS);
        }
        this.nodeID = localInt;
    }

    /**
     * Get last Access date-time (technically not available)
     * @return
     */
    public DateState getAccessDateTime()
    {
        return accessDateTime;
    }

    /**
     * Set last access date time
     * Technically not available
     * @param accessDateTime last access date time
     */
    public void setAccessDateTime(DateState accessDateTime)
    {
        this.accessDateTime = accessDateTime;
    }

    @Override
    public DateState getCreated()
    {
        return creationDateTime;
    }

    /**
     * Set creation date-time this node
     * @param creationDateTime creation date-time
     */
    public void setCreated(DateState creationDateTime)
    {
        this.creationDateTime = creationDateTime;
    }

    @Override
    public DateState getLastModified()
    {
        return lastModified;
    }

    /**
     * Set last update date-time this Node
     * @param updateDateTime last update date-time
     */
    public void setLastModified(DateState lastModified)
    {
        this.lastModified = lastModified;
    }

    @Override
    public long getNumFiles()
    {
        return numFiles;
    }

    /**
     * Set total number of Files in this node
     * @param numFiles total number of Files
     */
    public void setNumFiles(long numFiles)
    {
        this.numFiles = numFiles;
    }

    @Override
    public long getNumVersions()
    {
        return numVersions;
    }

    /**
     * Set total number of versions in this node
     * @param numVersions total number of versions
     */
    public void setNumVersions(long numVersions)
    {
        this.numVersions = numVersions;
    }

    @Override
    public long getNumObjects()
    {
        return numObjects;
    }

    /**
     * Set total number of objects in Node
     * @param numObjects number of objects
     */
    public void setNumObjects(int numObjects)
    {
        this.numObjects = numObjects;
    }

    /**
     * Get total number bytes this node
     * @return total number of bytes
     */
    public long getTotalSize() {
        return totalSize;
    }

    /**
     * Set Total number bytes for this node
     * @param totalSize number of files in this node
     */
    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }

    /**
     * Number of files - not specifically components
     * @return number of files
     */
    public long getNumActualFiles() {
        return numActualFiles;
    }

    /**
     * set number of file - not specifically components
     * @param numActualFiles
     */
    public void setNumActualFiles(long numActualFiles) {
        this.numActualFiles = numActualFiles;
    }

    /**
     * get the number of files - not specifically components
     * @return number of files
     */
    public long getTotalActualSize() {
        return totalActualSize;
    }

    public void setTotalActualSize(long totalActualSize) {
        this.totalActualSize = totalActualSize;
    }

    /**
     * get description of node
     * @return node description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Set description value
     * @param description node description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * get verifyOnRead switch
     *
     * @return true=option to do fixity on read, false=no fixity test should be performed
     */
    public boolean isVerifyOnRead() {
        return verifyOnRead;
    }

    /**
     * Set verifyOnRead switch
     * @param verifyOnReadS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    public void setVerifyOnRead(String verifyOnReadS)
        throws TException
    {

        this.verifyOnRead = testBoolean(verifyOnReadS, true);
    }

    @Override
    public String getBaseURI() {
        return baseURI;
    }

    /**
     * Set Access URI
     * @param accessURI Set Access URI
     */
    public void setBaseURI(String accessURI) {
        this.baseURI = accessURI;
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
     * get verifyOnWrite switch
     *
     * @return true=option to do fixity on write, false=no fixity test should be performed
     */
    public boolean isVerifyOnWrite() {
        return verifyOnWrite;
    }

    /**
     * Set verifyOnWrite switch
     * @param verifyOnWwriteS String form of verify - may be empty - i.e. is true
     * @throws TException invalid string value
     */
    public void setVerifyOnWrite(String verifyOnWriteS)
        throws TException
    {
        this.verifyOnWrite = testBoolean(verifyOnWriteS, true);
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
     * Set Date last Object deleted
     * @param Date last Object deleted
     */
    public void setLastDeleteObject(DateState lastDeleteObject) {
        this.lastDeleteObject = lastDeleteObject;
    }

    public DateState getLastDeleteVersion() {
        return lastDeleteVersion;
    }

    /**
     * Set Date last Version deleted
     * @param Date last Version deleted
     */
    public void setLastDeleteVersion(DateState lastDeleteVersion) {
        this.lastDeleteVersion = lastDeleteVersion;
    }

    public String getLogicalVolume() {
        return logicalVolume;
    }

    public void setLogicalVolume(String logicalVolume) {
        this.logicalVolume = logicalVolume;
    }

    public Integer getTargetNodeID() {
        return targetNodeID;
    }

    public void setTargetNodeID(int targetNodeID) {
        this.targetNodeID = targetNodeID;
    }

    public void setTargetNodeID(String targetNodeIDS)
        throws TException
    {
        System.out.println ("targetNodeIDS:" + targetNodeIDS);
        this.targetNodeID = setInt("setTargetNodeID", targetNodeIDS);
        System.out.println ("targetNode:" + this.targetNodeID);
    }

    public Integer getSourceNodeID() {
        return sourceNodeID;
    }

    public void setSourceNodeID(int sourceNodeID) {
        this.sourceNodeID = sourceNodeID;
    }

    public void setSourceNodeID(String sourceNodeIDS)
        throws TException
    {
        System.out.println ("sourceNodeIDS:" + sourceNodeIDS);
        this.sourceNodeID = setInt("setSourceNodeID", sourceNodeIDS);
        System.out.println ("sourcetNode:" + this.sourceNodeID);
    }

    /**
     * Validate passed String for true/false/yes/no
     * @param boolS boolean value as string
     * @param defaultVal if no string provided use this default
     * @return boolean value based on string value
     * @throws TException process exception
     */
    public boolean testBoolean(String boolS, boolean defaultVal)
        throws TException
    {
        boolean localBool = defaultVal;
        if (StringUtil.isNotEmpty(boolS)) {
                localBool = StringUtil.argIsTrue(boolS);
        }
        return localBool;
    }

    /**
     * Dump Node State
     * @param header display header
     * @return String dump of Node State
     */
    public String dump(String header)
    {
        StringBuffer buf = new StringBuffer();
        if (storageAccessProtocol != null) {
            buf.append(" - storageAccessProtocol type=" + storageAccessProtocol.getType() + "\n");
            buf.append(" - storageAccessProtocol name=" + storageAccessProtocol.getName() + "\n");
        }
        if (storageExternalProvider != null) {
            buf.append(" - storageExternalProvider type=" + storageExternalProvider.getType() + "\n");
            buf.append(" - storageExternalProvider name=" + storageExternalProvider.getName() + "\n");
        }
        if (storageMediaType != null) {
            buf.append(" - storageMediaType type=" + storageMediaType.getType() + "\n");
            buf.append(" - storageMediaType name=" + storageMediaType.getName() + "\n");
        }
        if (storageMediaConnectivity != null) {
            buf.append(" - storageMediaConnectivity type=" + storageMediaConnectivity.getType() + "\n");
            buf.append(" - storageMediaConnectivity name=" + storageMediaConnectivity.getName() + "\n");
        }
        return header + ":"
                    + " - nodeID=" + getIdentifier() + "\n"
                    + " - nodeName=" + getName() + "\n"
                    + " - nodeBranchScheme=" + getBranchScheme() + "\n"
                    + " - nodeClassScheme=" + getClassScheme() + "\n"
                    + " - nodeLeafScheme=" + getLeafScheme() + "\n"
                    + " - nodeProtocol=" + getNodeProtocol() + "\n"
                    + " - nodeStandard=" + getNodeScheme() + "\n"
                    + " - storageAccessType=" + getAccessMode() + "\n"
                    + " - accessProtocol=" + getAccessProtocol() + "\n"
                    + " - mediaType=" + getMediaType() + "\n"
                    + " - mediaConnectivity=" + getMediaConnectivity() + "\n"
                    + " - externalProvider=" + getExternalProvider() + "\n"
                    + " - description=" + getDescription() + "\n"
                    + " - verifyOnRead=" + isVerifyOnRead() + "\n"
                    + " - verifyOnWrite=" + isVerifyOnWrite() + "\n"
                    + " - BUF=\n" + buf.toString();
    }
    
    protected Integer setInt(String header, String idS)
        throws TException
    {
        int localInt = -1;
        if (StringUtil.isAllBlank(idS)) return null;
        try {
            idS = idS.trim();
            localInt = Integer.parseInt(idS);
        } catch (Exception ex) {
            throw new TException.INVALID_ARCHITECTURE(MESSAGE + header + " - invalid conversion for " + idS);
        }
        return localInt;
    }
    
    /**
     * The list of excluded producer files is included in store-info.txt
     * 
     * @return list of producer file names to be excluded
     * @throws TException 
     */
    public List setProducerFilter(Properties prop)
        throws TException
    {
        if (prop == null) {
            System.out.println("Load prop default - prop == null");
            return setProducerFilterDefault();
        }
        if (prop.getProperty("producerFilter.1") !=  null) {
            System.out.println("Load prop current");
            return extractFilter(prop);
        }
        String producerFilterFile = prop.getProperty("producerFilterFile");
        if (!StringUtil.isAllBlank(producerFilterFile)) {
            Properties filterProp = PropertiesUtil.loadPropertiesFileName(producerFilterFile);
            System.out.println("Load prop file:" + producerFilterFile);
            return extractFilter(filterProp);
        }
        System.out.println("Load prop default - prop != null");
        return setProducerFilterDefault();
    }
    
    protected List extractFilter(Properties prop)
        throws TException
    {
        ArrayList<String> producerFilter = new ArrayList();
        for(int i=1; i<=50; i++) {
            String value = prop.getProperty("producerFilter." + i);
            if (value != null) {
                producerFilter.add(value);
                System.out.println("setProducerFilter:" + value);
            } else break;
        }
        return producerFilter;
    }
    
    protected List setProducerFilterDefault()
        throws TException
    {
        System.out.println("setProducerFilterDefault");
        String [] shortlist  = {
                "mrt-erc.txt",
                "mrt-eml.txt",
                "mrt-dc.xml",
                "mrt-delete.txt",
                "mrt-dua.txt",
                "mrt-dataone-manifest.txt",
                "mrt-datacite.xml",
                "mrt-embargo.txt",
                "mrt-oaidc.xml",
                "stash-wrapper.xml"
            };
        ArrayList<String> producerFilter = new ArrayList();
        for (String name : shortlist) {
            producerFilter.add(name);
        }
        return producerFilter;
    }

    public List<String> retrieveProducerFilter() 
        throws TException
    {
        return setProducerFilter(nodeStateProp);
    }
}

