/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store.test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import org.cdlib.mrt.cloud.action.FixityFile;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;

import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.store.FileFixityState;
import org.cdlib.mrt.utility.MessageDigestType;

/**
 *
 * @author replic
 */
public class TestFileFixidy 
{

    protected static final String NAME = "TestFileURL";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected int sizeChecksumBuffer = 36000000;
    protected StorageConfig config = null;
    protected CloudStoreInf s3service = null;
    protected Long nodeNumber = null;
    protected Identifier objectID = null;
    protected String bucket = null;
    protected LoggerInf logger = null;
    
    
    public static void main(String[] args) 
            throws IOException,TException 
    {
        System.out.println("MinioTest2");
        String key = "ark:/28722/k23j3911v|1|system/mrt-mom.txt";
        
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        
        TestFileFixidy tvc = new TestFileFixidy(9502, logger);
        tvc.testComponent(
                "ark:/28722/bk0006w8m0c|1|producer/cabeurle_60_1_00037077.xml",
                5598,
                //5958,
                "d60c4a7b1250ba8b1f24dbc977d5f1eaa77687d23a06cd76bb09bcd691501e30"
        );
        if (true) return;
        tvc.testComponent( 
                "ark:/28722/bk0003f046h|1|producer/ark:/28722/bk0003f050q",
                113588420,
                "5bbe6c3df95c0fea0a8223a9af301f8ee30c1a2188a79a6f3a561d23a56ce20e"
        );
        
        
        tvc.testComponent( 
                "ark:/b5072/fk2377d54w|1|producer/enwiki-20170420-stub-meta-history24.xml.gz",
                2005108080,
                "dec7b1a20c1bc9fc4e9813f50b34c3cc5283cca88536076a502b1586e274ad03"
        );
        
            
        tvc.testComponent( // bad digest
                "ark:/b5072/fk2377d54w|1|producer/enwiki-20170420-stub-meta-history24.xml.gz",
                2005108080,
                "aaa7b1a20c1bc9fc4e9813f50b34c3cc5283cca88536076a502b1586e274ad03"
        );
        
        
        
            
        tvc.testComponent( //11G
                "ark:/99999/fk4x07b092|1|producer/5.10.HK.DanielRodrigez.02.mov",
                11189271438L,
                "664119c2c652045b69398bdedb02af16a1ca2789b17c036fbca19b983638a94a"
        );
    }
    
    public TestFileFixidy(long node, LoggerInf logger)
        throws TException
    {
        this.config = StorageConfig.useYaml();
        this.nodeNumber = node;
        NodeIO.AccessNode accessNode = config.getAccessNode(node);
        this.s3service = accessNode.service;
        this.bucket = accessNode.container;
        this.logger = logger;
    }
    
    protected void testComponent(String key, long size, String digestVal )
        throws TException
    {
        try {
            String parts[] = key.split("\\|", 3);
            FileComponent component = new FileComponent();
            component.setLocalID(key);
            component.setSize(size);
            component.setIdentifier(parts[0]);
            objectID = new Identifier(component.getIdentifier());
            int versionID = Integer.parseInt(parts[1]);
            component.setIdentifier(parts[2]);
            component.setLocalID(key);
            component.setFirstMessageDigest(digestVal, "sha256");
            FileFixityState fileState = getFileFixityState(objectID, versionID, parts[2]);
            String dump = fileState.dump("******");
            System.out.println(dump);
            
        } catch (Exception ex) {
            System.out.println("EXCEPTION:"
                    + " - key:" + key
                    + " - ex:" + ex
            );
        }
    }
    
    
    public FileFixityState getFileFixityState (
            //File objectStoreBase,
            Identifier objectID,
            int versionID,
            String fileName)
        throws TException
    {
        try {
            FixityFile fixityFile = FixityFile.getFixityFile(s3service, bucket, objectID, versionID, fileName, logger);
            System.out.println(MESSAGE + "getFileFixityState entered"
                    + " - objectID=" + objectID
                    + " - versionID=" + versionID
                    + " - fileName=" + fileName);
            return (FileFixityState)fixityFile.callEx();

        } catch (Exception ex) {
            throw new TException(ex);
        }
    } 
    
}
