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

import org.cdlib.mrt.cloud.action.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.cdlib.mrt.cloud.VersionMap;

import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.cloud.action.AddVersionThread;

import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.cloud.utility.NormVersionMap;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
/**
 * Run fixity
 * @author dloy
 */
public class TestAddVersionThread
{
    protected static final String NAME = "TestAddVersionThread";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String ARK = "ark:/13030/zyxwv08";
    protected static final String BUCKET = "dloy.bucket";

    
    public static void main(String[] args) throws TException {
        
        ManInfo test = new ManInfo();
        InputStream propStream =  test.getClass().getClassLoader().
                getResourceAsStream("testresources/SDSC-Openstack.properties");
        if (propStream == null) {
            System.out.println("Unable to find resource");
            return;
        }
        Properties xmlProp = new Properties();
        try {
            xmlProp.load(propStream);
            LoggerInf logLocal = new TFileLogger(NAME, 50, 50);
            //CloudStoreInf s3service = SDSCCloud.getSDSC(xmlProp, logLocal);
            CloudStoreInf s3service = OpenstackCloud.getOpenstackCloud(xmlProp, logLocal);
            //Identifier objectID = new Identifier("ark:/13030/fdhij");
            
            Identifier objectID = new Identifier(ARK);           
            DeleteObject deleteObject = DeleteObject.getDeleteObject(s3service, BUCKET, objectID, logLocal);
            deleteObject.callEx();
            
            System.out.println("AFTER delete");
            /*
            test(s3service,logLocal,
                "/replic/loy/tomcat/webapps/test/s3/qt1k67p66s/manifest1.txt",
                "dloy.bucket",
                ARK,
                "system/mrt-mom.txt"
                );
                */
            test(s3service,logLocal,
                "/replic/loy/tomcat/webapps/test/s3/qt1k67p66s/manifest1.txt",
                BUCKET,
                ARK,
                null
                );
            //if (true) return;
            /*
            test(s3service,logLocal,
                "/replic/loy/tomcat/webapps/test/s3/qt1k67p66s/manifest2.txt",
                "dloy.bucket",
                ARK,
                "system/mrt-erc.txt"
                );
                */
            test(s3service,logLocal,
                "/replic/loy/tomcat/webapps/test/s3/qt1k67p66s/manifest2.txt",
                BUCKET,
                ARK,
                null
                );
            test(s3service,logLocal,
                "/replic/loy/tomcat/webapps/test/s3/qt1k67p66s/manifest3.txt",
                BUCKET,
                ARK,
                null
                );
            
            NormVersionMap normMap = new NormVersionMap(s3service, logLocal);
            Identifier ark = new Identifier(ARK);
            VersionMap versionMap = normMap.getVersionMap(BUCKET,ark);
            String strMap = NormVersionMap.versionMap2String(versionMap);
            System.out.println("FINAL VersionMap:\n" + strMap);
            
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
    
    protected static void test(
            CloudStoreInf s3service,
            LoggerInf logLocal,
            String testFileS, 
            String bucketName,
            String objectIDS,
            String exceptionID
            )
    {
        try {
            System.out.println("************TEST**************\n"
                    + " - testFileS=" + testFileS + "\n"
                    + " - bucketName=" + bucketName + "\n"
                    + " - objectIDS=" + objectIDS + "\n"
                    + " - doException=" + exceptionID + "\n"
                    + "__________________________________________\n"
                    );
            //CloudStoreInf s3service = SDSCCloud.getSDSC(xmlProp, logLocal);
            Identifier objectID = new Identifier(objectIDS);
            File manifestFile = new File(testFileS);
            if (manifestFile.exists()) System.out.println("Using file:" + manifestFile.getAbsolutePath());
            InputStream manifestInputStream = new FileInputStream(manifestFile);
            AddVersionThread add = AddVersionThread.getAddVersionThread(s3service, bucketName, objectID, true, manifestInputStream, 10, logLocal);
            //add.setTESTEXCEPTION(exceptionID);
            VersionState state = add.process();
            System.out.println("\n**************************\nVersionState=" + add.formatXML(state));
            
        } catch (TException tex) {
            System.out.println(">>>>test TException:" + tex);
            
        } catch (Exception ex) {
            System.out.println(">>>>test Exception:" + ex);
        }
        
    }
}

