/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store.test;
import java.io.File;
import java.util.List;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.log.utility.Log4j2Util;
import org.cdlib.mrt.store.action.MatchObject;
import org.cdlib.mrt.cloud.VersionMap;
import static org.cdlib.mrt.store.test.TestMatchObject.NAME;
/**
 * 
 *
 * @author replic
 */
public class TestMatchObjectMap 
{
    protected static final String NAME = "TestMatchObject";
    protected static final String MESSAGE = NAME + ": ";
 
    public static void main(String[] args) 
            throws IOException,TException 
    {
        try {
            Log4j2Util.setRootLevel("debug");
            Log4j2Util.whichLog4j2("start");
            if (false) return;
            //Log4j2Util.setLoggerLevel("FixLog","trace");
            Log4j2Util.setLoggerLevel("FixLog","info");
            //long processNode = 9501;
            long processNode = 9501;
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            String yamlName = "yaml:";
            //String yamlName = "jar:nodes-sdsc-temp";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            //Identifier objectID = new Identifier("ark:/13030/m56t6p2z");
            //Identifier objectID = new Identifier("ark:/13030/m5dz7ggg");
            //Identifier objectID = new Identifier("ark:/13030/m5n40rb7");
            //Identifier objectID = new Identifier("ark:/13030/m5m38x40");
            //Identifier objectID = new Identifier("ark:/13030/m5bk6gpc");
            //Identifier objectID = new Identifier("ark:/13030/m5fc148t");
            //Identifier objectID = new Identifier("ark:/13030/m5rz4kds");
            //Identifier objectID = new Identifier("ark:/13030/qt0dd8d1c2"); // no changes
            //Identifier objectID = new Identifier("ark:/13030/m50057c3");
            if (false) return;
            Long nodeIDSource = 9502L;
            Identifier objectIDSource = new Identifier("ark:/20775/bb05469232");
            Long nodeIDTarget = 2002L;
            Identifier objectIDTarget = new Identifier("ark:/20775/bb05469232");
            File sourceMap = new File("/apps/replic/tasks/fix/240904-match/oldmap.xml");
            VersionMap sourceVersionMap = getVersionMap("/apps/replic/tasks/fix/240904-match/oldmap.xml", logger);
            VersionMap targetVersionMap = getVersionMap("/apps/replic/tasks/fix/240904-match/newmap.xml", logger);
            
            MatchObject matchObject = new MatchObject(sourceVersionMap, nodeIDSource, objectIDSource, 
                targetVersionMap, nodeIDTarget, objectIDTarget, logger);
            //matchObject.getSource().dump("source");
            //matchObject.getTarget().dump("target");
            //matchObject.matchSourceTarget();
            matchObject.matchDigest();
            matchObject.printStatus();
            matchObject.dumpDiffList("digest_fail");
            int testcnt = matchObject.getDiffCnt("digest_match");
            System.out.println("TEST digest_match:" + testcnt);
            
            testcnt = matchObject.getDiffCnt("digest_fail_count");
            System.out.println("TEST digest_fail_count:" + testcnt);
            
            testcnt = matchObject.getDiffCnt("digest_fail");
            System.out.println("TEST digest_fail:" + testcnt);
            
            
            List<MatchObject.DiffInfo> diffs = matchObject.getDiffList("digest_fail");
            for(MatchObject.DiffInfo diff : diffs) {
                String strDump = diff.fileComponent.dump("hdr");
                System.out.println("strDump=" + strDump);
                String fileID = diff.fileComponent.getIdentifier();
                if (fileID.equals("system/provenance_manifest.xml")) {
                    System.out.print("provenance found");
                } else {
                    System.out.print("provenance NOT found");
                }
            }
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            
        }
    }
    
    public static VersionMap getVersionMap(String mapFilePath, LoggerInf logger)
        throws TException
    {
        try {
            
            File fileMap = new File(mapFilePath);
            FileInputStream manifestXMLIn = new FileInputStream(fileMap);
            return  ManifestSAX.buildMap(manifestXMLIn, logger);
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
}