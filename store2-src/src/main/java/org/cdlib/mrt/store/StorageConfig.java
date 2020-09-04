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
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.NodeIO;
import static org.cdlib.mrt.store.StoreNodeManager.DEBUG;
import static org.cdlib.mrt.store.StoreNodeManager.MESSAGE;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.tools.YamlParser;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 *
 * @author dloy
 */
public class StorageConfig
{
    protected static final String NAME = "StorageConfig";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    
    protected File confBase = null;
    protected String baseURI = null;
    protected String supportURI = null;
    protected Boolean verifyOnRead = null;
    protected Boolean verifyOnWrite = null;
    protected ArrayList <String> producerFilter = new ArrayList();
    protected Long archiveNode = null;
    protected String nodePath = null;
    protected NodeIO nodeIO = null;
    protected LoggerInf logger = null;
    protected URL storeLink = null;
    protected Properties asyncArchivProp = null;
    //protected NodeIO.AccessNode archiveAccessNode = null;
    
    public static StorageConfig useYaml(String configEnv, LoggerInf logger)
        throws TException
    {
        try {
            if (logger == null) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "logger required");
            }
           
            if (StringUtil.isAllBlank(configEnv)) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "ENV name not supplied");
            }
            StorageConfig storageConfig = new StorageConfig(logger);
            String confPathS = System.getenv(configEnv);
            if (confPathS == null) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "ENV  not found:"  + configEnv);
            }
            File confBase = new File(confPathS);
            if (!confBase.exists()) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "base directory not found:" + confPathS);
            }
            File confFile = new File(confBase, "store-info.yml");
            if (!confFile.exists()) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "config file not found:" 
                        + confFile.getAbsolutePath());
            }
            storageConfig.setConfBase(confBase);
            String storeYaml = FileUtil.file2String(confFile);
        
            YamlParser yamlParser = new YamlParser();
            yamlParser.parseString(storeYaml);
            yamlParser.resolveValues();
            String jsonS = yamlParser.dumpJson();
            if (DEBUG) System.out.append("jsonS:" + jsonS);
            JSONObject jobj = new JSONObject(jsonS);
            JSONObject jStoreInfo = jobj.getJSONObject("store-info");
            storageConfig.setBaseURI(jStoreInfo.getString("baseURI"));
            storageConfig.setVerifyOnRead(jStoreInfo.getBoolean("verifyOnRead"));
            storageConfig.setVerifyOnWrite(jStoreInfo.getBoolean("verifyOnWrite"));
            String asyncArchivePropS = null;
            try {
                asyncArchivePropS = jStoreInfo.getString("asyncArchivProp");
                storageConfig.setAsyncArchiveProp(asyncArchivePropS);
            } catch (Exception ex) { }
            
            storageConfig.setAsyncArchiveProp(asyncArchivePropS);
            storageConfig.setNodePath(jStoreInfo.getString("nodePath"));
            storageConfig.setNodeIO(storageConfig.getNodePath());
            storageConfig.setArchiveNode(jStoreInfo.getLong("archiveNode"));
            storageConfig.setStoreLink();
            
            JSONArray jarr = jStoreInfo.getJSONArray("producerFilter");
            for (int i=0; i < jarr.length(); i++) {
                String producerFilter = jarr.getString(i);
                storageConfig.addProducerFilter(producerFilter);
            }
            return storageConfig;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    public StorageConfig(LoggerInf logger) 
            throws TException
    { 
        if (logger == null) {
            throw new TException.INVALID_CONFIGURATION(MESSAGE + "logger required");
        }
        this.logger = logger;
    }
    
    public NodeIO setNodeIO(String nodeIOPath)
        throws TException
    {
        try {
                nodeIO = NodeIO.getNodeIOConfig(nodeIOPath, logger);
                setNodeIO(nodeIO);
                return nodeIO;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void setStoreLink()
        throws TException
    {
        String accessURLS = getBaseURI();
        if (DEBUG) {
            System.out.println("!!!!storeLink accessURLS:" + accessURLS);

        if (StringUtil.isEmpty(accessURLS)) return;            }
        URL urlValue = null;
        try {
            urlValue = new URL(accessURLS);
        } catch (Exception ex) {
            throw new TException.INVALID_DATA_FORMAT(MESSAGE + "property value invalid:" + accessURLS);
        }
        this.storeLink = urlValue;
        if (DEBUG) {
            System.out.println("!!!!storeLink:" + this.storeLink.toString());
        }
    }
    
    public File getConfBase() {
        return confBase;
    }

    public void setConfBase(File confBase) {
        this.confBase = confBase;
    }
    

    public String getBaseURI() {
        return baseURI;
    }

    public void setBaseURI(String baseURI) {
        this.baseURI = baseURI;
    }

    public Boolean getVerifyOnRead() {
        return verifyOnRead;
    }

    public void setVerifyOnRead(Boolean verifyOnRead) {
        this.verifyOnRead = verifyOnRead;
    }

    public Boolean getVerifyOnWrite() {
        return verifyOnWrite;
    }

    public void setVerifyOnWrite(Boolean verifyOnWrite) {
        this.verifyOnWrite = verifyOnWrite;
    }

    public ArrayList<String> getProducerFilter() {
        return producerFilter;
    }

    public void setProducerFilter(ArrayList<String> producerFilter) {
        this.producerFilter = producerFilter;
    }

    public void addProducerFilter(String entry) {
        this.producerFilter.add(entry);
    }

    public Long getArchiveNode() {
        return archiveNode;
    }

    public void setArchiveNode(Long archiveNode) 
        throws TException
    {
        this.archiveNode = archiveNode;
    }

    public String getNodePath() {
        return nodePath;
    }

    public void setNodePath(String nodePath) 
        throws TException
    {
        if (DEBUG) System.out.println("%%%in  nodePath:" + nodePath);
        if (nodePath.contains("./")) {
            if (this.confBase == null) {
                throw new TException.INVALID_OR_MISSING_PARM("confBase required for relative file path");
            }
            nodePath = nodePath.replace("./", confBase.getAbsolutePath() + "/");
        }
        if (DEBUG) System.out.println("%%%out nodePath:" + nodePath);
        this.nodePath = nodePath;
    }

    public NodeIO getNodeIO() {
        return nodeIO;
    }

    protected void setNodeIO(NodeIO nodeIO)
        throws TException
    {
        if (nodeIO == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "NodeIO required");
        }
        this.nodeIO = nodeIO;
    }

    public URL getStoreLink() {
        return storeLink;
    }

    public String getSupportURI() {
        return supportURI;
    }

    public void setSupportURI(String supportURI) {
        this.supportURI = supportURI;
    }

    public void setAsyncArchiveProp(String asyncArchivePropS) {
        try {
            this.asyncArchivProp = null;
            if (asyncArchivePropS == null) return;
            asyncArchivePropS = asyncArchivePropS.replace("./", confBase.getAbsolutePath() + "/");
            File asyncArchivePropF = new File(asyncArchivePropS);
            if (!asyncArchivePropF.exists()) {
                this.asyncArchivProp = null;
                return;
            }
            this.asyncArchivProp = PropertiesUtil.loadFileProperties(asyncArchivePropF);
            
        } catch (Exception ex) {
            this.asyncArchivProp = null;
        }
    }

    public Properties getAsyncArchivProp() {
        return asyncArchivProp;
    }
    
    public LoggerInf getLogger() {
        return logger;
    }

    public List<NodeIO.AccessNode> getAccessNodes() {
        return nodeIO.getAccessNodesList();
    }

    public NodeIO.AccessNode getAccessNode(long node) 
    {
        return nodeIO.getAccessNode(node);
    }

    public String getNodeName() {
        if (nodeIO == null) return null;
        return nodeIO.getNodeName();
    }
    
    public String dump(String header)
    {
        StringBuffer buf = new StringBuffer("producerFilter\n");
        for (String name : producerFilter) {
            buf.append(" - " + name + "\n");
        }
        if (asyncArchivProp != null) {
            buf.append(PropertiesUtil.dumpProperties("\nAsyncArchiveProp", asyncArchivProp));
        } else {
            buf.append("\nAsyncArchiveProp: none");
        }
        
        String retString = header  + "\n"
                + " - confBase=" + getConfBase() + "\n"
                + " - baseURI=" + getBaseURI() + "\n"
                + " - verifyOnRead=" + getVerifyOnRead() + "\n"
                + " - verifyOnWrite=" + getVerifyOnWrite() + "\n"
                + " - verifyOnWrite=" + getVerifyOnWrite() + "\n"
                + " - archiveNode=" + getArchiveNode() + "\n"
                + " - nodePath=" + getNodePath() + "\n"
                + " - nodeName=" + getNodeName() + "\n"
                + " - producerFilter:" + "\n"
                + buf.toString() + "\n"
        ;
        
        return retString;
    }
    
    public static void main(String[] argv) {
    	
    	try {
            
            LoggerInf logger = new TFileLogger("test", 50, 50);
            String configEnv = "MERRITT_INV_INFO";
            StorageConfig storageConfig = StorageConfig.useYaml(configEnv, logger);
            System.out.println(storageConfig.dump("test"));
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
}

