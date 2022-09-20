/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.store.action.CloudArchive;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;

import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.utility.MessageDigestType;

/**
 *
 * @author replic
 */
public class TestCloudArchiveDelete 
{

    protected static final String NAME = "TestFileURL";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected int sizeChecksumBuffer = 36000000;
    protected StorageConfig config = null;
    protected CloudStoreInf s3service = null;
    protected Long nodeNumber = null;
    protected String bucket = null;
    protected LoggerInf logger = null;
    protected File workBase = null;
    
    
    public static void main(String[] args) 
            throws IOException,TException 
    {
        String arks_original[] = {
"ark:/28722/bk0006w8m0c",
"ark:/28722/bk0003f046h",
"ark:/28722/bk0003f037j",
"ark:/28722/bk0003f028k",
"ark:/28722/bk0003f0673",
"ark:/28722/bk00010877t",
"ark:/28722/bk00010879x",
"ark:/28722/bk000108811",
"ark:/28722/bk0003d6571",
"ark:/28722/bk0003d653t",
"ark:/28722/bk0003d925t",
"ark:/28722/bk0003d8k99",
"ark:/28722/bk0003d8k0b",
"ark:/28722/bk0003d6b3d",
"ark:/28722/bk0003d6b7m",
"ark:/28722/bk0003d955c",
"ark:/28722/bk0003d9q7r",
"ark:/28722/bk0003d685g",
"ark:/28722/bk0003d6818",
"ark:/28722/bk0003d689p",
"ark:/28722/bk0003d625c",
"ark:/28722/bk0003d629k",
"ark:/28722/bk0003d6215",
"ark:/28722/bk0003d6617",
"ark:/28722/bk0003d665f",
"ark:/28722/bk0003d669n",
"ark:/28722/bk0003d9k3g",
"ark:/28722/bk0003d5z37",
"ark:/28722/bk0003d5z7f",
"ark:/28722/bk0003d8v11",
"ark:/28722/bk0003d8q78",
"ark:/28722/bk0003d5w7d",
"ark:/28722/bk0003d5w36",
"ark:/28722/bk0003d9r6q",
"ark:/28722/bk0003d613r",
"ark:/28722/bk0003d617z",
"ark:/28722/bk0003d9z12",
"ark:/28722/bk0003d9p4k",
"ark:/28722/bk0003d9n5m",
"ark:/28722/bk0003d633s",
"ark:/28722/bk0003d6370",
"ark:/28722/bk0003d6996",
"ark:/28722/bk0003d8s8v",
"ark:/28722/bk0003d6c51",
"ark:/28722/bk0003d6c1t",
"ark:/28722/bk0003d9380",
"ark:/28722/bk0003d993b",
"ark:/28722/bk0003d5x91",
"ark:/28722/bk0003d5x5t",
"ark:/28722/bk0003d5x1m"
};
        
    String arks_gt_10[] = {
"ark:/99999/fk4cv4mm4g",
"ark:/b5072/fk22n55523",
"ark:/99999/fk4087hf78",
"ark:/99999/fk4vh6st3z",
"ark:/99999/fk4766w92x",
"ark:/99999/fk4r51244h",
"ark:/99999/fk4tx4r20q",
"ark:/99999/fk4r22br42",
"ark:/99999/fk4j97k513",
"ark:/99999/fk4dj6vb51",
"ark:/99999/fk4xk9qk0t",
"ark:/99999/fk4st90r87",
"ark:/99999/fk4p288z7d",
"ark:/99999/fk47m1t29r",
"ark:/99999/fk4gx5tx78",
"ark:/99999/fk4ww8xv85"
};
    String arks_part2[] = {
"ark:/99999/fk4kh24s00",
"ark:/99999/fk4475v771",
"ark:/99999/fk4bv8zc7f",
"ark:/99999/fk4kw6vt0q",
"ark:/99999/fk4j68xd0q",
"ark:/99999/fk40k3tt7c",
"ark:/99999/fk4m342k6x",
"ark:/99999/fk4qr6310m",
"ark:/b5072/fk22n55523",
"ark:/99999/fk4087hf78",
"ark:/99999/fk4j97k513",
"ark:/99999/fk4tx4r20q",
"ark:/99999/fk4st90r87",
"ark:/99999/fk4766w92x",
"ark:/99999/fk4r51244h",
"ark:/99999/fk4p288z7d",
"ark:/99999/fk4xk9qk0t"
};
    String arks[] = {
//"ark:/28722/k23j3911v"
            "ark:/99999/fk4ff5x9n"
};

        LoggerInf logger = new TFileLogger(NAME, 8, 50);
        File build = new File("/apps/replic1/temp/zipwork");
        TestCloudArchiveDelete test = new TestCloudArchiveDelete(9502, build, logger);
        //test.testComponentVersion("ark:/28722/bk0003d9q7r", 0, "zip");
        for (String ark : arks) {
            CAStats statunc = test.testComponentVersion(ark, 0, "zipunc");
            CAStats statcom = test.testComponentVersion(ark, 0, "zip");
            //procStat(ark, statunc, statcom);
        }
        
    }
    public static void main_object(String[] args) throws TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        
        Properties xmlProp = new Properties();
        try {
            Identifier objectID = new Identifier("ark:/b5072/fk24t6n929");
            NodeService nodeService = NodeService.getNodeService("nodes-dev", 5001, logger);
            CloudStoreInf service = nodeService.getCloudService();
            String bucket = nodeService.getBucket();
            String baseDirS = "/apps/replic/test/github/170919-store/base";
            File baseDir = new File(baseDirS);
            CloudArchive cloudArchive = new CloudArchive(service, bucket, objectID, baseDir,"zip",  logger);
            FileContent fileContent = cloudArchive.buildObject("mytest", false);
            //cloudArchive.buildVersion(3);
            //cloudArchive.buildProducer(3, null);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
        
    }
    
    public TestCloudArchiveDelete(
            long node, 
            File workBase,
            LoggerInf logger)
        throws TException
    {
        this.config = StorageConfig.useYaml();
        this.nodeNumber = node;
        NodeIO.AccessNode accessNode = config.getAccessNode(node);
        this.s3service = accessNode.service;
        this.bucket = accessNode.container;
        this.logger = logger;
        this.workBase = workBase;
        
    }
    
    public CAStats testComponentObject(
            String archiveName,
            String objectIDS,
            String archiveTypeS)
        throws TException
    {
        CloudArchive cloudArchive = null;
        long startMs = System.currentTimeMillis();
        try {
            Identifier objectID = new Identifier(objectIDS);
            cloudArchive = new CloudArchive(s3service, bucket, objectID, workBase, archiveTypeS, logger);
            File objzip = new File(workBase,"test-" + archiveTypeS + ".zip");
            FileOutputStream objstream = new FileOutputStream(objzip);
            cloudArchive.buildObject(objstream, false);
            long procMs = System.currentTimeMillis() - startMs;
            return setStats(cloudArchive, objzip, procMs);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public CAStats testComponentVersion(
            String objectIDS,
            int version,
            String archiveTypeS)
        throws TException
    {
        CloudArchive cloudArchive = null;
        long startMs = System.currentTimeMillis();
        try {
            Identifier objectID = new Identifier(objectIDS);
            File objzip = new File(workBase,"test-" + archiveTypeS + ".zip");
            FileOutputStream objstream = new FileOutputStream(objzip);
            cloudArchive = new CloudArchive(s3service, bucket, objectID, workBase, archiveTypeS, logger).setDeleteFileAfterCopy(true);
            System.out.println("testComponentVersion");
            cloudArchive.buildVersion(version, objstream);
            long procMs = System.currentTimeMillis() - startMs;
            return setStats(cloudArchive, objzip, procMs);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static void procStat(
            String objectIDS, CAStats unc, CAStats cmp)
        throws TException
    {
        try {
            float diffpu = (float) cmp.procMs / unc.procMs;
            float diffsz = (float) unc.size / cmp.size;
            System.out.println("***Stats "
                    + " - objectIDS=" + objectIDS
                    + " - diffpu=" + diffpu
                    + " - diffsz=" + diffsz
                    + " - procUnc=" + unc.procMs
                    + " - procCmp=" + cmp.procMs
                    + " - sizeUnc=" + unc.size
                    + " - sizeCmp=" + cmp.size
            );
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    public void dump(
            String objectIDS, String archiveTypeS, CloudArchive cloudArchive, File outzip, long procMs)
        throws TException
    {
        try {
            long addListMs = cloudArchive.getAddListMs();
            long buildMs = cloudArchive.getBuildMs();
            long size = outzip.length();
            System.out.println("***Stats "
                    + " - objectIDS=" + objectIDS
                    + " - archiveTypeS=" + archiveTypeS
                    + " - procMS=" + procMs
                    + " - size=" + size
                    + " - addListMs=" + addListMs
                    + " - buildMs=" + buildMs
            );
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public CAStats setStats (
            CloudArchive cloudArchive, File outzip, long procMs)
        throws TException
    {
        try {
            long addListMs = cloudArchive.getAddListMs();
            long buildMs = cloudArchive.getBuildMs();
            long size = outzip.length();
            return new CAStats(procMs, size, addListMs, buildMs);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static class CAStats {
        public long procMs = 0;
        public long size = 0;
        public long addListMs = 0;
        public long buildMs = 0;
        public CAStats (
            long procMs,
            long size,
            long addListMs,
            long buildMs)
        {
            this.procMs = procMs;
            this.size = size;
            this.addListMs = addListMs;
            this.buildMs = buildMs;
        }
        
    }
}
