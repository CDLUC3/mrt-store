package org.cdlib.mrt.store.test;

import java.io.File;

import org.cdlib.mrt.store.dflat.Dflat_1d0;
import org.cdlib.mrt.store.dflat.DflatVersionManagerAbs;
import org.cdlib.mrt.store.dflat.DflatInfo;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.store.dflat.DflatVersionReddManager;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;


public class TestRedd
{


    protected static final String NAME = "TestRedd";
    protected static final String MESSAGE = NAME + ": ";
    protected static LoggerInf logger = null;
    protected TFrame mFrame = null;
    protected DflatVersionReddManager versionManager = null;
    protected File storeFile = null;

    TestRedd(TFrame mFrame)
    {
        this.mFrame = mFrame;
    }

    /**
     * Main method
     */
    public static void main(String args[])
    {
        System.out.println("TestRedd entered");
        try
        {
            String propertyList[] = {
                "resources/MFrameDefault.properties",
                "resources/MFrameService.properties",
                "resources/FeederService.properties",
                "resources/IngestClient.properties",
                "resources/FeederClient.properties",
                "resources/MFrameLocal.properties"};
            TFrame mFrame = new TFrame(propertyList, NAME);
            logger = mFrame.getLogger();
            TestRedd test = new TestRedd(mFrame);
            test.run();

        }  catch(Exception e)  {
            if (logger != null)
            {
                logger.logError(
                    "Main: Encountered exception:" + e, 0);
                logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    protected void run()
    {
        try {
            String storeName = mFrame.getProperty(NAME + ".store");
            log("storeName=" + storeName, 0);
            storeFile = new File(storeName);
            if (!storeFile.exists()) {
                log("storeFile does not exist", 0);
                return;
            }
            DflatInfo dflatInfo = Dflat_1d0.getDflatInfoTxt();
            versionManager
                = DflatVersionManagerAbs.getDflatVersionReddManager(storeFile, logger, dflatInfo);

            String currentVersionName = mFrame.getProperty(NAME + ".currentVersion");
            log("currentVersion=" + currentVersionName, 0);
            File currentVersion = new File(storeFile, currentVersionName);
            if (!currentVersion.exists()) {
                log("currentVersion does not exist", 0);
                return;
            }

            String previousVersionName = mFrame.getProperty(NAME + ".previousVersion");
            log("previousVersion=" + previousVersionName, 0);
            File previousVersion = new File(storeFile, previousVersionName);
            if (!previousVersion.exists()) {
                log("previousVersion does not exist", 0);
                return;
            }

            File cp = testBuildDelta("cp", currentVersion, previousVersion);
            File pc = testBuildDelta("pc", previousVersion, currentVersion);
            File cc = testBuildDelta("cc", currentVersion, currentVersion);
            System.out.println("BuildFull*****************************************");
            testBuildFull("cpfull", currentVersion, cp);
            testBuildFull("pcfull", previousVersion, pc);
            testBuildFull("ccfull", currentVersion, cc);

        }  catch(Exception e)  {
                log("Main: Encountered exception:" + e, 0);
                log(StringUtil.stackTrace(e), 0);
        }
    }

    protected File testBuildDelta(String dirName, File current, File previous)
    {
        try {
            File outDir = new File(storeFile, dirName);
            outDir.mkdir();
            if (!outDir.exists()) {
                throw new TException.INVALID_ARCHITECTURE(
                        "mkdir fails");
            }
            log(outDir.getCanonicalPath(), 0);
            versionManager.buildDeltaPrevious(outDir, current, previous);
            return outDir;

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
            return null;
        }
    }

    protected File testBuildFull(String dirName, File current, File previous)
    {
        try {
            File outDir = new File(storeFile, dirName);
            outDir.mkdir();
            if (!outDir.exists()) {
                throw new TException.INVALID_ARCHITECTURE(
                        "mkdir fails");
            }
            log(outDir.getCanonicalPath(), 0);
            versionManager.buildFullPrevious(outDir, current, previous);
            return outDir;

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
            return null;
        }
    }


    protected static void log(String msg, int lvl)
    {
        System.out.println(msg);
        //logger.logMessage(msg, 0, true);
    }

}