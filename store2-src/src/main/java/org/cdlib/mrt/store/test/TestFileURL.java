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
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;

/**
 *
 * @author replic
 */
public class TestFileURL 
{

    protected static final String NAME = "TestFileURL";
    protected static final String MESSAGE = NAME + ": ";
    private LoggerInf logger = null;
    
    
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
        TestFileURL tfu = new TestFileURL(logger);
        tfu.testSetFile(fileUrl.toString());
        tfu.testSetFile("file:/apps/replic/tasks/store2/210603-opt/test.txt");
        tfu.testSetFile("file://apps/replic/tasks/store2/210603-opt/test.txt");
        tfu.testSetFile("file:///apps/replic/tasks/store2/210603-opt/test.txt");
        tfu.testSetFile("file:////apps/replic/tasks/store2/210603-opt/test.txt");
        tfu.testSetFile("file:///apps/replic/tasks/store2/210603-opt/xxx.txt");
        tfu.testSetFile("http://uc3-mrtstore01x2-stg.cdlib.org:35121/content/5001/ark%3A%2F99999%2Fstg01n5001a/1/system%2Fmrt-membership.txt?fixity=no");
        
    }
    
    public TestFileURL(LoggerInf logger)
        throws TException
    {
        this.logger = logger;
    }
    
    public void testSetFile(String uRLS)
            
    {
        try {
            System.out.println("\n***testSetFile:" + uRLS);
            FileComponent manifestComponent = new FileComponent();
            manifestComponent.setURL(uRLS);
            setFile(manifestComponent);
            File extFile = manifestComponent.getComponentFile();
            System.out.println("Canonical:" + extFile.getCanonicalPath());
            String ext = FileUtil.file2String(extFile);
            System.out.println("\n----------content:\n"
                    + ">>"+ ext + "<<----------\n"
            );

        } catch (TException tex) {
            tex.printStackTrace();
            System.out.println("TException tex:" + tex);

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Exception ex:" + ex);
        }
    }
    
    public void setFile(FileComponent manifestComponent)
        throws TException
    {
        try {
            if (manifestComponent.getComponentFile() != null) return;
            
            URL url = manifestComponent.getURL();
            if (url == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fillComponent - component URL missing");
            }
            File returnFile = null;
            if (url.toString().startsWith("file:/")) {
                returnFile = fileFromFileURL(url);
            } else {
                returnFile = fileFromURL(url, logger);
            }
            manifestComponent.setComponentFile(returnFile);


        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }
    
    public static File fileFromURL(URL url, LoggerInf logger)
        throws TException
    {
        try {
            if (url == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fillComponent - component URL missing");
            }
            File tmpFile = FileUtil.getTempFile("tmpxx", ".txt");
            FileUtil.url2File(logger, url, tmpFile);
            return tmpFile;


        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }
    
    protected static File fileFromFileURL(URL fileURL)
        throws TException
    {
        try {
            if (fileURL == null) {
                throw new TException.INVALID_OR_MISSING_PARM("getFile null");
            }
            System.out.println("TEST URL:" + fileURL.toString());
            URI fileURI = fileURL.toURI();
            File bldFile = null;
            try {
                bldFile = new File(fileURI);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM("File URL invalid:" + fileURL.toString());
            }
            if (bldFile.exists()) {
                System.out.println("File exists");
                return bldFile;
            } else {
                throw new TException.INVALID_OR_MISSING_PARM("File does not exist:" + fileURL.toString());
            }

        } catch (TException tex) {
            System.out.println(tex);
            throw tex;

        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }
}
