/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.je;

import org.cdlib.mrt.store.PrimaryIDState;
import org.cdlib.mrt.store.PrimaryIDStateInf;
import org.cdlib.mrt.store.LocalIDsState;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.sleepycat.je.DatabaseException;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.je.Transaction;
import org.cdlib.mrt.core.Checkm;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;

/**
 *
 * @author dloy
 */
public class LocalIDDatabase
{
    protected static final String NAME = "LocalIDDatabase";
    protected static final String MESSAGE = NAME + ":";
    protected static final boolean DEBUG = false;
    protected final static String NL = System.getProperty("line.separator");
    public final String OUTDELIM = " | ";
    public final String SPLITDELIM = "\\s*\\|\\s*";

    protected File dbEnvPath = null;
    private static File localIDFile = null;
    private static File dumpFile = null;

    protected LoggerInf logger = null;
    protected DataAccessor da;

    // Encapsulates the environment and data store.
    protected DbEnv dbEnv = new DbEnv();


    protected LocalIDDatabase(LoggerInf logger)
    {
        this.logger = logger;
    }

    public static LocalIDDatabase getLocalIDDatabase(
            LoggerInf logger,
            File dbEnvPath,
            boolean readOnly)
        throws TException
    {
        return new LocalIDDatabase(logger, dbEnvPath, readOnly);
    }

    protected LocalIDDatabase(
            LoggerInf logger,
            File dbEnvPath,
            boolean readOnly)
        throws TException
    {
        this.logger = logger;
        if ((dbEnvPath == null)
            || !dbEnvPath.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM (
                    MESSAGE + "LocalIDDatabase dbEnvPath required");
        }
        if (DEBUG) {
            try {
                System.out.println("***getLocalIDDatabase"
                        + " - dbEnvPath:" + dbEnvPath.getCanonicalPath()
                        + " - readOnly:" + readOnly
                        );
            } catch (Exception ex) { }
        }
        this.dbEnvPath = dbEnvPath;
        setupDb(readOnly);
    }

    public void setupDb(boolean readOnly)
    {
        dbEnv.setup(dbEnvPath,  // Path to the environment home
                      readOnly);       // Environment read-only?

        // Open the data accessor. This is used to store
        // persistent objects.
        da = new DataAccessor(dbEnv.getEntityStore());
        System.out.println("setupDb readOnly=" + readOnly);
    }

    public void closeDb()
    {
        dbEnv.close();
        System.out.println("closeDb");
    }

    public Transaction beginTransaction()
    {
        return dbEnv.getEnv().beginTransaction(null, null);
    }

    public void commitTransaction(Transaction txn)
    {
        txn.commit();
    }

    public void abortTransaction(Transaction txn)
    {
        txn.abort();
    }

    public boolean writeDb(String context, String localID, String objectID)
        throws TException
    {
        try {
            LocalIDMap localIDMap = new LocalIDMap();
            if (DEBUG) System.out.println("Add:"
                    + " - context=" + context
                    + " - localID=" + localID
                    + " - objectID=" + objectID
                    );
            localIDMap.setLocalID(context, localID, objectID);
            boolean writeOut = da.primaryIndex.putNoOverwrite(localIDMap);
            if (DEBUG) System.out.println("put"
                    + " - theLocalID:"
                    + localIDMap.getContextLocalID()
                    + " - write=" + writeOut
                    );

 
            return writeOut;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public LocalIDMap readLocalDb(String context, String localID)
        throws TException
    {
        try {
            String key = LocalIDMap.getID(context, localID);
            return da.primaryIndex.get(key);


        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }
    public LocalIDMap readPrimaryDb(String primaryID)
        throws TException
    {
        try {
            return da.secondaryIndex.get(primaryID);


        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public boolean deleteLocalID(String context, String localID)
        throws TException
    {
        try {
            String key = LocalIDMap.getID(context, localID);
            return da.primaryIndex.delete(key);


        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }


    public boolean deletePrimaryID(String primaryID)
        throws TException
    {
        try {
            LocalIDsState localIDs = readPrimaryArrayDb(primaryID);
            return da.secondaryIndex.delete(primaryID);


        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }


    public LocalIDsState readPrimaryArrayDb(String primaryID)
        throws TException
    {
        LocalIDsState ids = new LocalIDsState();
        ids.setPrimaryIdentifier(primaryID);
        EntityCursor<LocalIDMap> map_cursor = null;
        try {
            map_cursor =
                    da.secondaryIndex.subIndex(primaryID).entities();
            for (LocalIDMap localIDMap : map_cursor) {
                ids.addLocalID(localIDMap);
            }
            return ids;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            map_cursor.close();
        }
    }

    public PrimaryIDState getPrimaryIDState(String context, String localID)
        throws TException
    {
        try {
            PrimaryIDState primaryIDState 
                    = new PrimaryIDState(context, localID);
            LocalIDMap localMap = readLocalDb(context, localID);
            if (localMap == null) return primaryIDState;
            else {
                String key = localMap.getContextLocalID();
                if (DEBUG) System.out.println("LocalIDMap:"
                        + " - " + key + "*=*" + localMap.dump(key)
                        );
                primaryIDState =
                        new PrimaryIDState(localMap);

                if (DEBUG) System.out.println("primaryIDState:"
                        + " - " + key + "*=*" + primaryIDState.dump(key)
                        );
            }
            return primaryIDState;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public PrimaryIDState getPrimaryIDState(String primaryID)
        throws TException
    {
        try {
            PrimaryIDState primaryIDState
                    = new PrimaryIDState(primaryID);
            LocalIDMap localMap = readPrimaryDb(primaryID);
            if (localMap == null) return primaryIDState;
            else {
                String key = localMap.getContextLocalID();
                System.out.println("getPrimaryIDState - LocalIDMap:"
                        + " - " + key + "*=*" + localMap.dump(key)
                        );
                primaryIDState =
                        new PrimaryIDState(localMap);

                System.out.println("getPrimaryIDState - primaryIDState:"
                        + " - " + key + "*=*" + primaryIDState.dump(key)
                        );
            }
            return primaryIDState;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public void dumpDb(File outFile)
        throws TException
    {
        EntityCursor<LocalIDMap> map_cursor = null;
        OutputStream os = null;
        OutputStreamWriter osw = null;
        try {
            if (outFile == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "dumpDb - outFile required");
            }
            os = new FileOutputStream(outFile);
            osw = new OutputStreamWriter(os, "utf-8");
            map_cursor =
                    da.secondaryIndex.entities();
            for (LocalIDMap localIDMap : map_cursor) {
                String context = localIDMap.getContext();
                if (context == null) context = "";
                String localID = localIDMap.getLocalID();
                if (localID == null) localID = "";
                String primaryID = localIDMap.getPrimaryID();
                if (primaryID == null) primaryID = "";
                String out = context + OUTDELIM + localID + OUTDELIM + primaryID + NL;
                osw.write(out);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            map_cursor.close();
            try {
                osw.close();
            } catch (Exception e) {}
            try {
                os.close();
            } catch (Exception e) {}
        }
    }

    public void dumpDbApp(File outFile)
        throws TException
    {
        EntityCursor<LocalIDMap> map_cursor = null;
        try {
            map_cursor =
                    da.secondaryIndex.entities();
            for (LocalIDMap localIDMap : map_cursor) {
                appendBackup(
                    localIDMap.getContext(),
                    localIDMap.getLocalID(),
                    localIDMap.getPrimaryID(),
                    outFile);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            map_cursor.close();
        }
    }

    public void appendBackup(
            String context,
            String localID,
            String primaryID,
            File outFile)
        throws TException
    {
        OutputStream os = null;
        OutputStreamWriter osw = null;
        try {
            if (outFile == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "dumpDb - outFile required");
            }
            os = new FileOutputStream(outFile, true);
            osw = new OutputStreamWriter(os, "utf-8");
            String out = backupDbEntry(
                    context,
                    localID,
                    primaryID
                    );
            osw.write(out);

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);

        } finally {
            try {
                osw.close();
            } catch (Exception e) {}
            try {
                os.close();
            } catch (Exception e) {}
        }
    }

    public String backupDbEntry(
            String context,
            String localID,
            String primaryID)
        throws TException
    {

        try {

            if (context == null) context = "";
            if (localID == null) localID = "";
            if (primaryID == null) primaryID = "";
            return context + OUTDELIM + localID + OUTDELIM + primaryID + NL;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);

        }
    }



    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        LoggerInf logger = null;
        LocalIDDatabase test = null;
        try
        {
            String propertyList[] = {
                "testresources/JELocal.properties"};
            framework = new TFrame(propertyList, NAME);
            logger = framework.getLogger();
            test = new LocalIDDatabase(logger);
            test.runBuild(framework.getProperties());
            test.runReadLocal(framework.getProperties());
            test.runReadPrimary(framework.getProperties());
            test.runReadPrimaryMulti(framework.getProperties());
            test.runDump(framework.getProperties());
            test.runDumpApp(framework.getProperties());


        }  catch(Exception e)  {
            System.out.println("Exception:"  + e);
            e.printStackTrace();

        } finally {
            test.closeDb();
        }
        System.out.println("All done.");
    }

    private void runBuild(Properties props)
        throws TException
    {
        System.out.println("****runBuild****");
        setFiles(props);
        setupDb(false);
        System.out.println("loading local ID");
        loadLocalIDDbTransaction();
        closeDb();
    }


    private void runReadLocal(Properties props)
        throws TException
    {
        System.out.println("****runReadLocal****");
        setFiles(props);
        setupDb(true);
        System.out.println("loading local ID");
        readLocalDb();
        closeDb();
    }


    private void runReadPrimary(Properties props)
        throws TException
    {
        System.out.println("****runReadPrimary****");
        setFiles(props);
        setupDb(true);
        System.out.println("loading local ID");
        readPrimaryDb();
        closeDb();
    }


    private void runReadPrimaryMulti(Properties props)
        throws TException
    {
        System.out.println("****runReadPrimary****");
        setFiles(props);
        setupDb(true);
        System.out.println("loading local ID");
        readPrimaryDbMulti();
        closeDb();
    }


    private void runDump(Properties props)
        throws TException
    {
        System.out.println("****runDump****");
        setFiles(props);
        setupDb(true);
        System.out.println("loading local ID");
        dumpDb(dumpFile);
        closeDb();
        String dumpFileS = FileUtil.file2String(dumpFile);
        System.out.println("****runDump****");
        System.out.println(dumpFileS);
    }

    private void runDumpApp(Properties props)
        throws TException
    {
        System.out.println("****runDump****");
        setFiles(props);
        setupDb(true);
        System.out.println("loading local ID");
        dumpDbApp(dumpFile);
        closeDb();
        String dumpFileS = FileUtil.file2String(dumpFile);
        System.out.println("****runDumpApp****");
        System.out.println(dumpFileS);
    }


    private void setFiles(Properties props)
        throws TException
    {
        String name = NAME + ".dbEnvPath";
        String myDbEnvPathS = props.getProperty(name);
        if (StringUtil.isEmpty(myDbEnvPathS)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + name + " - not found");
        }
        dbEnvPath = new File(myDbEnvPathS);

        name = NAME + ".localIDFile";
        String localIDFileS = props.getProperty(name);
        if (StringUtil.isEmpty(localIDFileS)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + name + " - not found");
        }
        localIDFile = new File(localIDFileS);

        name = NAME + ".dumpFile";
        String dumpFileS = props.getProperty(name);
        if (StringUtil.isEmpty(dumpFileS)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + name + " - not found");
        }
        dumpFile = new File(dumpFileS);
        try {
        System.out.println("Files:"
                + " - dbEnvPath=" + dbEnvPath.getCanonicalPath()
                + " - localIDFile=" + localIDFile.getCanonicalPath()
                );
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
        }

    }

    public void readLocalDb()
            throws TException
    {
        try {
            // loadFile opens a flat-text file that contains our data
            // and loads it into a list for us to work with. The integer
            // parameter represents the number of fields expected in the
            // file.
            List vendors = loadFile(localIDFile, 3);

            // Now load the data into the store.
            for (int i = 0; i < vendors.size(); i++) {
                String[] sArray = (String[])vendors.get(i);
                PrimaryIDStateInf primaryIDState
                        = getPrimaryIDState(sArray[0], sArray[1]);
                if (primaryIDState == null) {
                    System.out.println("primaryIDState null");
                } else {
                    System.out.println("primaryIDState:"
                            + " - "  + primaryIDState.dump("[" + i + "]")
                            );
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    private void readPrimaryDb()
            throws TException
    {
        try {
            // loadFile opens a flat-text file that contains our data
            // and loads it into a list for us to work with. The integer
            // parameter represents the number of fields expected in the
            // file.
            List vendors = loadFile(localIDFile, 3);

            // Now load the data into the store.
            for (int i = 0; i < vendors.size(); i++) {
                String[] sArray = (String[])vendors.get(i);
                PrimaryIDStateInf primaryIDState
                        = getPrimaryIDState(sArray[2]);
                if (primaryIDState == null) {
                    System.out.println("readPrimaryDb - primaryIDState null");
                } else {
                    System.out.println("readPrimaryDb - primaryIDState:"
                            + " - "  + primaryIDState.dump("[" + i + "]")
                            );
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    private void readPrimaryDbMulti()
            throws TException
    {
        try {

            // loadFile opens a flat-text file that contains our data
            // and loads it into a list for us to work with. The integer
            // parameter represents the number of fields expected in the
            // file.
            List vendors = loadFile(localIDFile, 3);

            // Now load the data into the store.
            for (int i = 0; i < vendors.size(); i++) {
                String[] sArray = (String[])vendors.get(i);
                LocalIDsState ids = readPrimaryArrayDb(sArray[2]);
                if (ids == null) {
                    System.out.println("readPrimaryDbMulti - null");
                } else {
                    System.out.println(ids.dump("readPrimaryDbMulti id=" + sArray[2]));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public void loadLocalIDDbTransaction()
            throws TException
    {
        // Start a transaction. All inventory items get loaded using a
        // single transaction.
        Transaction txn = beginTransaction();
        try {
            // loadFile opens a flat-text file that contains our data
            // and loads it into a list for us to work with. The integer
            // parameter represents the number of fields expected in the
            // file.
            List vendors = loadFile(localIDFile, 3);
            // Now load the data into the store.
            for (int i = 0; i < vendors.size(); i++) {
                String[] sArray = (String[])vendors.get(i);
                System.out.println("Add:"
                        + " - a0=" + sArray[0]
                        + " - a1=" + sArray[1]
                        + " - a2=" + sArray[2]
                        );
                boolean writeOut = writeDb(sArray[0], sArray[1], sArray[2]);
            }
            commitTransaction(txn);

        } catch (Exception ex) {
            abortTransaction(txn);
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    private List loadFile(File theFile, int numFields) {
        List<String[]> records = new ArrayList<String[]>();
        try {
            String theLine = null;
            FileInputStream fis = new FileInputStream(theFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            while((theLine=br.readLine()) != null) {
                String[] theLineArray = theLine.split(SPLITDELIM);
                if (theLineArray.length != numFields) {
                    System.out.println("Malformed line found in " + theFile.getPath());
                    System.out.println("Line was: '" + theLine);
                    System.out.println("length found was: " + theLineArray.length);
                    System.exit(-1);
                }
                records.add(theLineArray);
            }
            // Close the input stream handle
            fis.close();
        } catch (FileNotFoundException e) {
            System.err.println(theFile.getPath() + " does not exist.");
            e.printStackTrace();
        } catch (IOException e)  {
            System.err.println("IO Exception: " + e.toString());
            e.printStackTrace();
            System.exit(-1);
        }
        return records;
    }

    public File getDbEnvPath() {
        return dbEnvPath;
    }
}
