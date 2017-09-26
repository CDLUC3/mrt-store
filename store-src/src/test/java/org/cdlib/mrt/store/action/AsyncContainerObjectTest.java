/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.action;

import org.cdlib.mrt.store.je.*;
import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.cdlib.mrt.utility.FileUtil;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.store.LocalIDsState;

import static org.junit.Assert.*;

/**
 *
 * @author dloy
 */
public class AsyncContainerObjectTest
{

    protected static final String NAME = "AsyncContainerObjectTest";
    protected static final String MESSAGE = NAME + ": ";
    //x-ark:/13030/j2rf56vz | Huynh_10095 | ark:/13030/m59s1p0r
    //x-ark:/13030/j2rf56vz | Snead_6534 | ark:/13030/m5rx9913
    //x-ark:/13030/j2rf56vz | Arrenberg_10295 | ark:/13030/m5jd4tqb
    protected static final String CONTEXT = "ark:/13030/j2rf56vz";
    protected static final String LOCALID = "Snead_6534";
    protected static final String IDMAPDIR = "C:/Documents and Settings/dloy/My Documents/MRTMaven/repository/fixity/node/admin/idmap";
    protected static final String PRIMARYID = "ark:/13030/m5jd4tqb";
    private LoggerInf logger = null;
    private File tempDir = null;
    private File testFile = null;

    public AsyncContainerObjectTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    public void setEnv(String resourceMap) {
        try {
            if (logger == null) {
                logger = LoggerAbs.getTFileLogger("testFormatter", 10, 10);
            }
            tempDir = FileUtil.getTempDir("tmp");
            InputStream stream = getResource(resourceMap);
            testFile = new File(tempDir, "idmapback.txt");
            FileUtil.stream2File(stream, testFile);

        } catch (Exception ex) {
            logger = null;
        }
    }

    public void setEnv(String idmapFileName, String idmapDirectory)
    {
        try {
            if (logger == null) {
                logger = LoggerAbs.getTFileLogger("testFormatter", 10, 10);
            }

        } catch (Exception ex) {
            logger = null;
        }
    }


    @Test
    public void defaultTest()
    {
        assertTrue(true);
    }

    @Test
    public void testMessage()
    {
        try {
            String msg = "Collection: UCI Library Dash Object: ark:/b7280/d1js3b\n" +
"Date: 2016-09-06 15:35:24 -0700\n" +
"\n" +
"The version that you requested is ready for you to download.\n" +
"Please click on the link to download your file:\n" +
"\n" +
"  http://merritt.cdlib.org/container/1033641266.tar.gz\n" +
"\n" +
"Please note that this link will expire in 7 days from the date of this email.  \n" +
"The content is stored as a compressed file in the \"tar.gz\" format. For an explanation of\n" +
"how to extract the files in this container, see http://www.gzip.org/#faq6.\n" +
"If you have any questions regarding the download of this archive, please contact uc3@cdlib.org.";
            String out = AsyncContainerObject.insertHost(msg, "1033641266.tar.gz");
            System.out.println("OUT:\n" + out);
            
            assertTrue(true);

        } catch (Exception ex) {
            assertFalse("Exception:" + ex, true);
        }
    }

    //@Test
    public void testStorageDeleteIDLocal()
    {
        //ark:/13030/j2rf56vz | Gearhart_6934 | ark:/13030/m5610x8b
        try {
            StorageIDClient client = new StorageIDClient();
            Properties result = client.getPrimaryID(
                    "http://localhost:28080/storage",
                    "10",
                    CONTEXT,
                    LOCALID,
                    "xml",
                    10000
                    );
            System.out.println(PropertiesUtil.dumpProperties("testStorageDeleteID-get1", result));
            assertTrue(true);

            result = client.deleteLocalID(
                   "http://localhost:28080/storage",
                    "10",
                    CONTEXT,
                    LOCALID,
                    "xml",
                    10000
                    );
            System.out.println(PropertiesUtil.dumpProperties("testStorageDeleteID-delete", result));
            assertTrue(true);


            result = client.getPrimaryID(
                    "http://localhost:28080/storage",
                    "10",
                    CONTEXT,
                    LOCALID,
                    "xml",
                    10000
                    );
            System.out.println(PropertiesUtil.dumpProperties("testStorageDeleteID-get2", result));
            assertTrue(true);

        } catch (Exception ex) {
            assertFalse("Exception:" + ex, true);
        }
    }

    //@Test
    public void testStorageDeleteIDPrimary()
    {
        //ark:/13030/j2rf56vz | Gearhart_6934 | ark:/13030/m5610x8b
        try {
            StorageIDClient client = new StorageIDClient();
            Properties result = client.getLocalIDs(
                    "http://localhost:28080/storage",
                    "10",
                    PRIMARYID,
                    "xml",
                    10000
                    );
            System.out.println(PropertiesUtil.dumpProperties("testStorageDeleteID-get1", result));
            assertTrue(true);

            result = client.deletePrimaryID(
                   "http://localhost:28080/storage",
                    "10",
                    PRIMARYID,
                    "xml",
                    10000
                    );
            System.out.println(PropertiesUtil.dumpProperties("testStorageDeleteID-delete", result));
            assertTrue(true);


            result = client.getLocalIDs(
                    "http://localhost:28080/storage",
                    "10",
                    PRIMARYID,
                    "xml",
                    10000
                    );
            System.out.println(PropertiesUtil.dumpProperties("testStorageDeleteID-get2", result));
            assertTrue(true);

        } catch (Exception ex) {
            assertFalse("Exception:" + ex, true);
        }
    }

    public InputStream getResource(String resourceName)
        throws TException
    {
        try
        {
            return getClass().getClassLoader().
                getResourceAsStream("testresources/" + resourceName);
        }
        catch(Exception e)
        {
               System.out.println(
                "MFrame: Failed to get the AdminManager for entity: " +
                "Failed to get resource: " +
                resourceName +
                " Exception: " + e);
           throw new TException.GENERAL_EXCEPTION(
                "Failed to get a resource. Exception: " + e);
        }
    }
}