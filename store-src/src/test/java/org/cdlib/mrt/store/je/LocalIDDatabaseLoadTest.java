/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.je;

import java.io.File;
import java.io.InputStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.cdlib.mrt.utility.FileUtil;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.store.LocalIDsState;

import static org.junit.Assert.*;

/**
 *
 * @author dloy
 */
public class LocalIDDatabaseLoadTest
{

    protected static final String NAME = "LocalIDDatabaseLoadTest";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String IDMAPFILE = "C:/Documents and Settings/dloy/My Documents/MRTMaven/work/store/mrt-storage/store-src/src/test/resources/testresources/idmapback.txt";
    protected static final String IDMAPDIR = "C:/Documents and Settings/dloy/My Documents/MRTMaven/repository/fixity/node/admin/idmap";
    protected static final String PRIMARYID = "ark:/13030/m5z0365x";
    private LoggerInf logger = null;
    private File tempDir = null;
    private File testFile = null;

    public LocalIDDatabaseLoadTest() {
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
            testFile = new File(idmapFileName);
            tempDir = new File(idmapDirectory);

        } catch (Exception ex) {
            logger = null;
        }
    }

    //@Test
    public void testSearchLocal()
    {

        try {
            setEnv(IDMAPFILE, IDMAPDIR);
            LocalIDDatabaseLoad loader
                    = LocalIDDatabaseLoad.getLocalIDDatabaseLoad(logger, tempDir);
            LocalIDDatabase db = loader.getLocalIDDatabase();
            LocalIDsState localIDs = db.readPrimaryArrayDb(PRIMARYID);
            System.out.println(localIDs.dump("test"));
            assertTrue(true);

        } catch (Exception ex) {
            assertFalse("Exception:" + ex, true);
        }
    }

    //@Test
    public void buildMap()
    {

        try {
            setEnv(IDMAPFILE, IDMAPDIR);
            LocalIDDatabaseLoad loader
                    = LocalIDDatabaseLoad.getLocalIDDatabaseLoad(logger, tempDir);
            loader.loadLocalIDDb(testFile);
            assertTrue(true);

            boolean verify = loader.verifyDb(testFile);
            assertTrue(verify);

        } catch (Exception ex) {
            assertFalse("Exception:" + ex, true);
        }
    }

    //@Test
    public void testFullMap()
    {

        try {
            setEnv("idmapback.txt");
            LocalIDDatabaseLoad loader
                    = LocalIDDatabaseLoad.getLocalIDDatabaseLoad(logger, tempDir);
            loader.loadLocalIDDb(testFile);
            assertTrue(true);

            boolean verify = loader.verifyDb(testFile);
            assertTrue(verify);
            
        } catch (Exception ex) {
            assertFalse("Exception:" + ex, true);
        }
    }

    @Test
    public void testDelete()
    {

        try {
            setEnv("idmapshort.txt");
            LocalIDDatabaseLoad loader
                    = LocalIDDatabaseLoad.getLocalIDDatabaseLoad(logger, tempDir);
            loader.loadLocalIDDb(testFile);
            assertTrue(true);

            boolean verify = loader.verifyDb(testFile);
            assertTrue(verify);

            LocalIDDatabase db = loader.getLocalIDDatabase();
            boolean deleted = db.deleteLocalID("ark:/13030/j2c821zz", "doi:10.5060/D2D798B9");
            assertTrue(deleted);
            System.out.println("Delete localID - ark:/13030/j2c821zz doi:10.5060/D2D798B9");

            verify = loader.verifyDb(testFile);
            assertFalse(verify);
            int fail = loader.getMatchFailCnt();
            assertTrue(fail == 1);

            deleted = db.deletePrimaryID("ark:/13030/m5tx3c9v");
            assertTrue(deleted);
            System.out.println("Delete primaryID - ark:/13030/m5tx3c9v");
            verify = loader.verifyDb(testFile);
            fail = loader.getMatchFailCnt();
            assertTrue(fail == 2);

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