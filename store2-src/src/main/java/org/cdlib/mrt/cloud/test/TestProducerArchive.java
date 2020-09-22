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
package org.cdlib.mrt.cloud.test;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.ManifestSAX;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.action.ProducerComponentList;


import org.cdlib.mrt.store.KeyFileInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeService;
/**
 * Run fixity
 * @author dloy
 */
public class TestProducerArchive
{
    protected static final String NAME = "TestAddVersionThread";
    protected static final String MESSAGE = NAME + ": ";

    
    public static void main(String[] args) 
            throws TException 
    {
        String baseDirS = "/apps/replic/test/dash/170201-store-producer/basePut";
        String containerDirS = "/apps/replic/test/dash/170201-store-producer/";
        String archiveName = "myarchivename";
        Identifier ark = new Identifier("ark:/99999/fk4hm5gt15");
        String archiveType = "targz";
        long node = 9001;
        //Identifier ark = new Identifier("ark:/99999/fk4rf60z8c");
        int version = 2;
        String [] shortlist  = {
                "mrt-erc.txt",
                "mrt-eml.txt",
                "mrt-dc.txt",
                "mrt-delete.txt",
                "mrt-dua.txt",
                "mrt-dataone-manifest.txt",
                "mrt-datacite.xml",
                "mrt-oaidc.xml"
            };
        
        ArrayList<String> filterList = new ArrayList<>();
        for (String name : shortlist) {
            filterList.add(name);
        }
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        //String key= "ark:/13030/m5vb1cpg|1|system/mrt-erc.txt";
        String nodeName = "nodes-stg";
        File baseDir = new File(baseDirS);
        File containerDir = new File(containerDirS);
        try {
            NodeService service= NodeService.getNodeService(nodeName, node, logger);
            GetKey getKey = new GetKey(service);
            VersionMap versionMap = getVersionMap(service, ark);
            ProducerComponentList producerEdit 
                    = new ProducerComponentList(ark, version, archiveName, baseDir, versionMap, filterList, logger);
            producerEdit.process(getKey, archiveType, false);
        /*
            producerEdit.edit();
            producerEdit.addFiles(getKey);
            FileContent producerArchive = producerEdit.buildArchive("targz");
            System.out.println(producerArchive.dump("test"));
            producerEdit.deleteArchiveDir();
        */
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    
    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    protected static VersionMap getVersionMap(NodeService service, Identifier objectID)
            throws TException
    {
        try {
            LoggerInf logger = service.getLogger();
            InputStream manifestXMLIn = service.getManifest(objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static class GetKey
        implements KeyFileInf
    {
        private NodeService service = null;
        public GetKey(NodeService service)
            throws TException
        {
            this.service= service;
                    
        }
        public void keyToFile (
                String key, File outFile)
        throws TException
        {
            if (key == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - key required");
            }
            CloudResponse response = new CloudResponse();
            try {
                
                InputStream inStream = service.getObject(key, response);
                FileUtil.stream2File(inStream, outFile);

            } catch (TException me) {
                throw me;

            } catch (Exception ex) {
                System.out.println("KeyToFile Exception:" + ex
                        + " - key:" + key
                        
                );
                if (outFile != null) {
                    try {
                        System.out.println("KeyToFile file:" + ex
                                + " - outFile:" + outFile.getCanonicalPath()

                        );
                    } catch (Exception ext) { }
                    
                }
                throw new TException(ex);
            }
        }
    }
    
}

