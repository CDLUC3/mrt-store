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

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.StateInf;

/**
 * Node State interface
 * @author dloy
 */
public interface NodeStateInf
        extends StateInf
{
    // Property names
    public static final String CNAME = "name";
    public static final String CBRANCHSCHEME = "branchScheme";
    public static final String CCLASSSCHEME = "classScheme";
    public static final String CLEAFSCHEME = "leafScheme";
    public static final String CNODESCHEME = "nodeScheme";
    public static final String CBRANCHVERSION = "branchVersion";
    public static final String CCLASSVERSION = "classVersion";
    public static final String CLEAFVERSION = "leafVersion";
    public static final String CNODEVERSION = "nodeVersion";
    public static final String CACCESSMODE = "accessMode";
    public static final String CACCESSPROTOCOL = "accessProtocol";
    public static final String CNODEPROTOCOL = "nodeProtocol";
    public static final String CEXTERNALPROVIDER = "externalProvider";
    public static final String CMEDIACONNECTIVITY = "mediaConnectivity";
    public static final String CMEDIATYPE = "mediaType";
    public static final String CIDENTIFIER = "identifier";
    public static final String CDESCRIPTION = "description";
    public static final String CVERIFYONREAD = "verifyOnRead";
    public static final String CVERIFYONWRITE = "verifyOnWrite";
    public static final String CBASEURI = "baseURI";
    public static final String CSUPPORTURI = "supportURI";
    public static final String CLOGICALVOLUME = "logicalVolume";
    public static final String CNODEFORM = "nodeForm";
    public static final String CSOURCENODE = "sourceNode";
    public static final String CTARGETNODE = "targetNode";
    public static final String CTESTOK = "testOk";

    /**
     * Get assigned Node Name
     * @return Node Name
     */
    public String getName();

    /**
     * Unique identifier for this specific CAN
     * @return NodeID
     */
    public int getIdentifier();


    /**
     * get description of node
     * @return node description
     */
    public String getDescription();

    /**
     * get scheme of node
     * @return node scheme
     */
    public String getNodeScheme();

    /**
     * Get Implementation Version
     * @return implementation
     */
    public String getVersion();

    /**
     * Branch Scheme used for processing (e.g. pairtree)
     * @see org.cdlib.mrt.store.OpenLocationInf
     * @return Branch Scheme
     */
    public String getBranchScheme();

    /**
     * Leaf Scheme interface (e.g. dflat)
     * @see org.cdlib.mrt.store.OpenStoreInf
     * @return Leaf Scheme
     */
    public String getLeafScheme();

    /**
     * Node class scheme
     * @return class Scheme
     */
    public String getClassScheme();

    /**
     * Get Storage Media Type - readable description of storage
     * hardware type
     * @see  org.cdlib.mrt.core.StorageTypesEnum
     * @return Storage Media Type
     */
    public String getMediaType();

    /**
     * Get Storage Access Type - expected access response time
     * category
     * @see  org.cdlib.mrt.core.StorageTypesEnum
     * @return Storage Media Type
     */
    public String getAccessMode();

    /**
     * Get Storage Access Protocol - generic storage type
     * @see  org.cdlib.mrt.core.StorageTypesEnum
     * @return Storage Media Type
     */
    public String getAccessProtocol();

    /**
     * Get Storage External Provider - sdsc, amazon
     * @see  org.cdlib.mrt.core.StorageTypesEnum
     * @return Storage Media Type
     */
    public String getExternalProvider();

    /**
     * Get Generic storage type
     * @see  org.cdlib.mrt.core.StorageTypesEnum
     * @return Storage Media Type
     */
    public String getMediaConnectivity();

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
     * Get creation date-time for this Node
     * Corresponds to the date-time of the node directory file
     * @return creation date-time for this Node
     */
    public DateState getCreated();

    /**
     * Get last update date-time for this Node.
     * Corresponds to the date-time for last update to the node counter file
     * log/summary-stats.txt
     * @returnlast update date-time
     */
    public DateState getLastModified();

    /**
     * get the Access URI
     * @return Access URI
     */
    public String getBaseURI();

    /**
     * get the Support URI
     * @return Support URI
     */
    public String getSupportURI();


    /**
     * Date of last added version
     * @return date last added version
     */
    public DateState getLastAddVersion();

    /**
     * Date of last deleted version
     * @return date last deleted version
     */
    public DateState getLastDeleteVersion();   
    
    /**
     * Node form
     * @return physical, virtual, unknown
     */
    public String getNodeForm();

    /**
     * Node protocol - communication form between storage and node
     * @return file, http
     */
    public String getNodeProtocol();
    
    /**
     * do full state test
     * @return null=do test; true=do test; false=skip full state test
     */
    public Boolean getTestOk();
    
    /**
     * test node
     * @return status of node: null=unknown,true=running,false=error occurred
     */
    public Boolean getOk();
    
    /**
     * test error
     * @return null=no error; !null=error
     */
    public String getError();
    
    public String dump(String header);
}
