package org.cdlib.mrt.store.test;

import java.io.File;
import java.util.Vector;

import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TLockFile;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.store.pairtree.PairTree;
import org.cdlib.mrt.store.ObjectLocationInf;


public class TestPairtree
{


    protected static final String NAME = "TestPairtree";
    protected static final String MESSAGE = NAME + ": ";

    protected ObjectLocationInf pairtree = null;
    protected LoggerInf logger = null;
    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        try
        {
            String propertyList[] = {
                "testresources/TestLocal.properties"};

            TFrame mFrame = new TFrame(propertyList, NAME);
            TestPairtree test = new TestPairtree(mFrame);
            test.runIt();

        }  catch(Exception e)  {
            if (framework != null)
            {
                framework.getLogger().logError(
                    "Main: Encountered exception:" + e, 0);
                framework.getLogger().logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    public TestPairtree(TFrame mFrame)
        throws TException
    {
        this.logger = mFrame.getLogger();
        String rootS = mFrame.getProperty(NAME + ".directory");
        File root = new File(rootS);
        this.pairtree = new PairTree(logger, root);
    }

    public void runIt()
    {
        try {
            build("abcdef");
            build("abc");
            build("abcd");
            Identifier id = new Identifier("abcdef");
            pairtree.removeObjectLocation(id);

        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            System.out.println(MESSAGE + "Trace:" + StringUtil.stackTrace(ex));
        }
    }

    protected void build(String testval)
    {
        try {
            Identifier id = new Identifier(testval);
            File endpair = pairtree.buildObjectLocation(id);
            for (int i=1; i <= 10; i++) {
                File leaf = new File(endpair, "leaf." + i + ".txt");
                FileUtil.string2File(leaf, testval);
                System.out.println("create: leaf=" + leaf.getName());
            }

        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            System.out.println(MESSAGE + "Trace:" + StringUtil.stackTrace(ex));
        }
    }
}