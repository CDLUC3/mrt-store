/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store.test;
import java.io.IOException;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.log.utility.Log4j2Util;
import org.cdlib.mrt.store.fix.BuildTokenCC;
/**
 * 
 *
 * @author replic
 */
public class TestCCBuild 
{
    protected static final String NAME = "TestChangeTokenFix";
    protected static final String MESSAGE = NAME + ": ";
 
    public static void main(String[] args) 
            throws IOException,TException 
    {
        try {
            Log4j2Util.setRootLevel("info");
            Log4j2Util.whichLog4j2("start");
            if (false) return;
            //Log4j2Util.setLoggerLevel("FixLog","trace");
            Log4j2Util.setLoggerLevel("FixLog","info");
            //long processNode = 9501;
            long processNode = 9501;
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            String yamlName = "jar:nodes-sdsc-temp";
            String collection = "testCollection";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            Identifier objectID = new Identifier("ark:/13030/m56t6p2z");
            //Identifier objectID = new Identifier("ark:/13030/m5dz7ggg");
            //Identifier objectID = new Identifier("ark:/13030/m5n40rb7");
            //Identifier objectID = new Identifier("ark:/13030/m5m38x40");
            //Identifier objectID = new Identifier("ark:/13030/m5bk6gpc");
            //Identifier objectID = new Identifier("ark:/13030/m5fc148t");
            //Identifier objectID = new Identifier("ark:/13030/m5rz4kds");
            //Identifier objectID = new Identifier("ark:/13030/qt0dd8d1c2"); // no changes
            //Identifier objectID = new Identifier("ark:/13030/m50057c3");
            if (false) return;
            
            BuildTokenCC buildTokenCC = new BuildTokenCC(
                collection,
                nodeIO, 
                processNode, 
                objectID, 
                false,
                logger);
            buildTokenCC.process();
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            
        }
    }
}