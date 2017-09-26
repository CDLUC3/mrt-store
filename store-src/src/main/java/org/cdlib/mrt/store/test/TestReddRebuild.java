package org.cdlib.mrt.store.test;

import java.io.File;

import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.store.dflat.Dflat_1d0;
import org.cdlib.mrt.store.dflat.DflatInfo;
import org.cdlib.mrt.store.dflat.DflatManager;
import org.cdlib.mrt.store.dflat.DflatVersionManagerAbs;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;


public class TestReddRebuild
{


    protected static final String NAME = "TestReddRebuild";
    protected static final String MESSAGE = NAME + ": ";
    protected static LoggerInf logger = null;
    protected TFrame mFrame = null;
    protected DflatManager dflatManager = null;
    protected DflatVersionManagerAbs versionManager = null;
    protected File storeFile = null;

    TestReddRebuild(TFrame mFrame)
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
            TestReddRebuild test = new TestReddRebuild(mFrame);
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
            dflatManager = new DflatManager(storeFile, logger, dflatInfo, true, true);
            versionManager = dflatManager.getDflatVersionManager();
            File recover = new File(storeFile, "recover");
            if (recover.exists()) {
                FileUtil.deleteDir(recover);
            }
            recover.mkdir();
            int nextVersion = versionManager.getNextVersion();
            if (nextVersion == 1) {
                System.out.println("No version exists");
            }
            for (int ver=nextVersion-1; ver >= 1; ver--) {
                String verName = DflatVersionManagerAbs.getVersionName(ver);
                File verFile = new File(recover, verName);
                verFile.mkdir();
                versionManager.getFullVersion(ver, verFile);
                log("Created:" + verFile.getCanonicalPath(), 0);
                try {
                    dflatManager.validateFullVersion(ver, verFile);
                } catch (Exception ex) {
                    System.out.println("Exception - " + ver + ":" + ex);
                }
                try {
                    dflatManager.fixityFullVersion(ver, verFile);
                } catch (Exception ex) {
                    System.out.println("Exception - " + ver + ":" + ex);
                }
            }

        }  catch(Exception e)  {
                log("Main: Encountered exception:" + e, 0);
                log(StringUtil.stackTrace(e), 0);
        }
    }


    protected static void log(String msg, int lvl)
    {
        System.out.println(msg);
        //logger.logMessage(msg, 0, true);
    }

}