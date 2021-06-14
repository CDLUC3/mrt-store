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
import org.cdlib.mrt.utility.MessageDigestType;

/**
 *
 * @author replic
 */
public class TestValidateComponent 
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
        
        FileComponent manifestComponent = new FileComponent();
        File file = new File("/apps/replic/tasks/store2/210603-opt/test.txt");
        URL fileUrl = file.toURI().toURL();
		System.out.println("URL:" + fileUrl);
        TestValidateComponent tvc = new TestValidateComponent(9502, logger);
        tvc.testComponent(
                1,
                "ark:/28722/bk0006w8m0c|1|producer/cabeurle_60_1_00037077.xml",
                5598,
                //5958,
                "d60c4a7b1250ba8b1f24dbc977d5f1eaa77687d23a06cd76bb09bcd691501e30"
        );
        tvc.testComponent( // fail size
                1,
                "ark:/28722/bk0006w8m0c|1|producer/cabeurle_60_1_00037077.xml",
                5958,
                "d60c4a7b1250ba8b1f24dbc977d5f1eaa77687d23a06cd76bb09bcd691501e30"
        );
        tvc.testComponent( // fail digest
                1,
                "ark:/28722/bk0006w8m0c|1|producer/cabeurle_60_1_00037077.xml",
                5598,
                //5958,
                "aa0c4a7b1250ba8b1f24dbc977d5f1eaa77687d23a06cd76bb09bcd691501e30"
        );
        tvc.testComponent( 
                1,
                "ark:/28722/bk0003f046h|1|producer/ark:/28722/bk0003f050q",
                113588420,
                "5bbe6c3df95c0fea0a8223a9af301f8ee30c1a2188a79a6f3a561d23a56ce20e"
        );
        tvc.testComponent(  // digest error
                1,
                "ark:/28722/bk0003f046h|1|producer/ark:/28722/bk0003f050q",
                113588420,
                "aaaa6c3df95c0fea0a8223a9af301f8ee30c1a2188a79a6f3a561d23a56ce20e"
        );
        tvc.testComponent( 
                1,
                "ark:/28722/bk0003f046h|1|producer/ark:/28722/bk0003f050q",
                113588000,
                "5bbe6c3df95c0fea0a8223a9af301f8ee30c1a2188a79a6f3a561d23a56ce20e"
        );
        
        
        tvc.testComponent( 
                1,
                "ark:/b5072/fk2377d54w|1|producer/enwiki-20170420-stub-meta-history24.xml.gz",
                2005108080,
                "dec7b1a20c1bc9fc4e9813f50b34c3cc5283cca88536076a502b1586e274ad03"
        );
        
            
        tvc.testComponent( // bad digest
                1,
                "ark:/b5072/fk2377d54w|1|producer/enwiki-20170420-stub-meta-history24.xml.gz",
                2005108080,
                "aaa7b1a20c1bc9fc4e9813f50b34c3cc5283cca88536076a502b1586e274ad03"
        );
        
        
        
            
        tvc.testComponent( //11G
                1,
                "ark:/99999/fk4x07b092|1|producer/5.10.HK.DanielRodrigez.02.mov",
                11189271438L,
                "664119c2c652045b69398bdedb02af16a1ca2789b17c036fbca19b983638a94a"
        );
    }
    
    public TestValidateComponent(long node, LoggerInf logger)
        throws TException
    {
        this.config = StorageConfig.useYaml();
        this.nodeNumber = node;
        NodeIO.AccessNode accessNode = config.getAccessNode(node);
        this.s3service = accessNode.service;
        this.bucket = accessNode.container;
        this.logger = logger;
    }
    
    protected void testComponent(int versionID, String key, long size, String digestVal )
        throws TException
    {
        try {
            String parts[] = key.split("\\|", 3);
            FileComponent component = new FileComponent();
            component.setLocalID(key);
            component.setSize(size);
            component.setIdentifier(parts[0]);
            objectID = new Identifier(component.getIdentifier());
            int version = Integer.parseInt(parts[1]);
            component.setIdentifier(parts[2]);
            component.setLocalID(key);
            component.setFirstMessageDigest(digestVal, "sha256");
            validateComponent(version, component);
        } catch (Exception ex) {
            System.out.println("EXCEPTION:"
                    + " - key:" + key
                    + " - ex:" + ex
            );
        }
    }
    
    protected void validateComponent(int versionID, FileComponent file)
        throws TException
    {
        try {
            String key = file.getLocalID();
            if (DEBUG) System.out.println("***validateComponent entered"
                    + " - key=" + key
                    );
            MessageDigest digest = file.getMessageDigest();
            String checksumType = digest.getJavaAlgorithm();
            String checksum = digest.getValue();
            long fileSize = file.getSize();
            
            String [] types = new String[1];
            types[0] = checksumType;

            CloudChecksum cc = CloudChecksum.getChecksums(types, s3service, bucket, key, sizeChecksumBuffer);

            cc.process();
            CloudChecksum.CloudChecksumResult fixityResult
                    = cc.validateSizeChecksum(checksum, checksumType, fileSize, logger);
            String returnChecksum = cc.getChecksum(checksumType);
           
            String msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + file.getIdentifier()
                        + " - key=" + key
                        + " - checksumType=" + checksumType
                        + " - manifestChecksum=" + checksum
                        + " - manifestSize=" + file.getSize()
                        + " - returnChecksum=" + returnChecksum
                        + " - returnSize=" + cc.getInputSize()
                        + " - " + fixityResult.dump("ValidateComponent");
            if (!fixityResult.fileSizeMatch || !fixityResult.checksumMatch) {
                logger.logMessage("Component fixity FAILS:" + msg, 0, true);
                throw new TException.FIXITY_CHECK_FAILS("Component fixity FAILS " + msg);
                
            } else {
                    msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                        + " - fileID=" + file.getIdentifier()
                        + " - key=" + key;
                logger.logMessage("Component fixity OK:" + msg, 0, true);
                if (DEBUG) System.out.println("validateComponent OK:" + msg);
            }
            
        } catch (TException tex) {
            throw tex;

        } catch(Exception ex) {
            throw new TException(ex);
            
        }
    }
    
}
