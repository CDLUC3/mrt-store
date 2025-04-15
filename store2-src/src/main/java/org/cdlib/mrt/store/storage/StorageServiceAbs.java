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
import java.net.URL;
import java.util.Properties;

import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.store.StoreNodeManager;
import org.cdlib.mrt.store.StoreNode;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Common StorageService processes
 * @author dloy
 */
public class StorageServiceAbs
{
    protected static final String NAME = "StorageServiceAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected LoggerInf logger = null;
    protected StorageConfig storageConfig = null;
    protected StoreNodeManager nodeManager = null;
    //protected Properties confProp = null;

    /**
     * StorageService Factory
     * @param logger debug/status logging
     * @param confProp system properties
     * @return StorageService
     * @throws org.cdlib.mrt.utility.MException
     */
    public static StorageService getStorageService(
            StorageConfig storageConfig)
        throws TException
    {
        StorageService storageService = new StorageService(storageConfig);
        return storageService;
    }

    /**
     * Constructor
     * @param logger process log
     * @param confProp configuration properties
     * @throws TException process exception
     */
    public StorageServiceAbs(
            StorageConfig storageConfig)
        throws TException
    {
        if (storageConfig == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Required confProp is missing");
        }
        this.storageConfig = storageConfig;
        this.logger = storageConfig.getLogger();
        //System.out.println(PropertiesUtil.dumpProperties("!!!!: StorageServiceAbs:", confProp));
        this.nodeManager = StoreNodeManager.getStoreNodeManager(logger, storageConfig);
        if (this.nodeManager.getNodeCount() == 0) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "No Nodes found");
        }
    }

    /**
     * Return default NodeID used for setting CAN functionality
     * @return default NodeID
     */
    public Integer getDefaultNodeID()
    {
        return nodeManager.getDefaultNodeID();
    }

    /**
     * Set the properties for StoreNode to use the Default node
     * @return default nodeID
     * @throws TException default node not set or default URL not set
     */
    public Integer setDefaultNodeID()
        throws TException
    {
        
        Integer storageID = getDefaultNodeID();
        if ((storageID == null) || (storageID < 0)) {
            throw new TException.INVALID_DATA_FORMAT(MESSAGE
                    + "default nodeID required for this function");
        }
        StoreNode defaultNode = nodeManager.getStoreNode(storageID);
        URL nodeURL = defaultNode.getNodeLink();
        if (nodeURL == null) {
            throw new TException.INVALID_DATA_FORMAT(MESSAGE
                    + "default nodeID URL required for this function - be sure to add baseURI to can-info.txt");
        }
        nodeManager.setStorageLink(nodeURL);
        return storageID;
    }

    /**
     * Return StorageService level logger
     * @return logger
     */
    public LoggerInf getLogger()
    {
        return logger;
    }

    public StorageConfig getStorageConfig() {
        return storageConfig;
    }

    public Integer getAwsVersion() {
        if (storageConfig == null) return null;
        return storageConfig.getAwsVersion();
    }
}

