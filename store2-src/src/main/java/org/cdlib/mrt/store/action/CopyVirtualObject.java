/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
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
*******************************************************************************/
package org.cdlib.mrt.store.action;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionData;
import org.cdlib.mrt.store.ObjectState;

import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.store.NodeInf;
import org.cdlib.mrt.utility.DateUtil;
/**
 * Run fixity
 * @author dloy
 */
public class CopyVirtualObject
        extends ActionAbs
        implements Callable, Runnable
{

    protected static final String NAME = "CopyVirtualObject";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected static final boolean DISPLAYMAP = true;
    protected static final String WRITE = "write";

    protected NodeInf sourceNode = null;
    protected NodeInf targetNode = null;
    protected String storageBase = null;
    protected Identifier objectID = null;
    protected VersionMap sourceMap = null;
    protected int current = 0;
    protected ObjectState objectState = null;
    
    public static CopyVirtualObject getCopyVirtualObject(
            String storageBase,
            NodeInf sourceNode,
            NodeInf targetNode,
            Identifier objectID,
            LoggerInf logger)
        throws TException
    {
        return new CopyVirtualObject(storageBase, sourceNode, targetNode, objectID, logger);
    }
    
    protected CopyVirtualObject(
            String storageBase,
            NodeInf sourceNode,
            NodeInf targetNode,
            Identifier objectID,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.storageBase = storageBase;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.objectID = objectID;
        if (DEBUG) {
            System.out.println(MESSAGE + "****start - " + DateUtil.getCurrentIsoDate());
        }
        
        sourceMap = getVersionMap(sourceNode);
        current = sourceMap.getCurrent();
        if (DEBUG) System.out.println("CopyObject numberVersion=" + current);
    }
    
    protected void copyObject()
        throws TException
    {
        File outputDir = null;
        try {
            if (logger == null) System.out.println(MESSAGE + "logger null");
            else System.out.println(MESSAGE + "logger NOT null");
            outputDir = FileUtil.getTempDir("copydir");
            VersionData versionData = new VersionData(
                sourceMap, outputDir, logger);
            versionData.buildManifests();
            System.out.println("Process versionData");
            int current = versionData.getCurrent();
            File manifest = new File(outputDir, "manifest");
            for (int v=1; v <= current; v++) {
                String name = "version-" + v + ".txt";
                File manifestFile = new File(manifest, name);
                targetNode.addVersion(objectID, null, null, manifestFile);
                System.out.println("addversion:" + v);
            }
            objectState = targetNode.getObjectState(objectID);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
            try {
                FileUtil.deleteDir(outputDir);
            } catch (Exception ex) { 
                System.out.println("Exception deleting temp directory:" + ex);
            }
        }
    }
    
    public VersionMap getVersionMap(NodeInf fromNode)
        throws TException
    {
        try {
            FileContent mapContent = fromNode.getCloudManifest(objectID, false);
            File mapFile = mapContent.getFile();
            InputStream manifestXMLIn = new FileInputStream(mapFile);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            VersionMap map =  ManifestSAX.buildMap(manifestXMLIn, logger);
            map.setNode(sourceNode.getNodeID());
            map.setStorageBase(storageBase);
            return map;

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw makeGeneralTException(logger, "getVersionMap", ex);
        }
    }
    
    @Override
    public void run()
    {
        try {
            log("run entered");
            copyObject();
            
        } catch (Exception ex) {
            String msg = MESSAGE + "CopyVirtualObject Exception for "
                    + " - objectID=" + objectID
                    + " - Exception:" + ex
                    ;
            logger.logError(msg, 2);
            setException(ex);

        }

    }

    @Override
    public ObjectState call()
    {
        run();
        return objectState;
    }


    public ObjectState callEx()
        throws TException
    {
        run();
        if (exception != null) {
            if (exception instanceof TException) {
                throw (TException) exception;
            } else {
                throw new TException(exception);
            }
        }
        return objectState;
    }
}

