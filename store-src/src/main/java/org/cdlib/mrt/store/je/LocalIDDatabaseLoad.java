/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.je;

import org.cdlib.mrt.store.PrimaryIDStateInf;
import org.cdlib.mrt.store.LocalIDsState;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;


import com.sleepycat.je.Transaction;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.store.ContextLocalID;

/**
 *
 * @author dloy
 */
public class LocalIDDatabaseLoad
{
    protected static final String NAME = "LocalIDDatabaseLoad";
    protected static final String MESSAGE = NAME + ":";
    protected static final boolean DEBUG = false;
    protected final static String NL = System.getProperty("line.separator");
    public final String OUTDELIM = " | ";
    public final String SPLITDELIM = "\\s*\\|\\s*";
    public final int NUMFIELDS = 3;

    protected File dbEnvPath = null;
    private static File dumpFile = null;

    protected LoggerInf logger = null;
    protected LocalIDDatabase localIDDatabase = null;
    protected int loadCnt = 0;
    protected int matchCnt = 0;
    protected int matchFailCnt = 0;
    //protected int verifyCnt = 0;


    public static LocalIDDatabaseLoad getLocalIDDatabaseLoad(
            LoggerInf logger,
            File dbEnvPath)
        throws TException
    {
        return new LocalIDDatabaseLoad(logger, dbEnvPath);
    }

    protected LocalIDDatabaseLoad(
            LoggerInf logger,
            File dbEnvPath)
        throws TException
    {
        this.logger = logger;
        localIDDatabase
                = LocalIDDatabase.getLocalIDDatabase(logger, dbEnvPath, false);
    }

    public void loadLocalIDDb(File localIDFile)
            throws TException
    {
        String line = null;
        Transaction txn = localIDDatabase.beginTransaction();
        loadCnt = 0;
        try {
            FileInputStream fis = new FileInputStream(localIDFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            while((line=br.readLine()) != null) {
                String[] elements = line.split(SPLITDELIM);
                if (elements.length != NUMFIELDS) {
                    System.out.println("Malformed line found in " + localIDFile.getPath());
                    System.out.println("Line was: '" + line);
                    System.out.println("length found was: " + elements.length);
                    System.exit(-1);
                }
                String msg = "Elements:"
                        + " - line=" + line
                        + " - e0=" + elements[0]
                        + " - e1=" + elements[1]
                        + " - e2=" + elements[2];
                if (elements[0].startsWith("***")) {
                    boolean deleteOut = localIDDatabase.deleteLocalID(elements[0], elements[1]);
                    if (!deleteOut) {
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "deletion exception for:"
                                + " - line=" + line
                                + " - msg=" + msg
                                );
                    }

                } else {
                    boolean writeOut = localIDDatabase.writeDb(elements[0], elements[1], elements[2]);
                    if (!writeOut) {
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "loading exception for:"
                                + " - line=" + line
                                + " - msg=" + msg
                                );
                    }
                }
                if (DEBUG) System.out.println(msg);
                loadCnt++;
            }
            localIDDatabase.commitTransaction(txn);
            String msg = MESSAGE + "Table built"
                    + " - records added:" + loadCnt
                    + " - load file:" + localIDFile.getCanonicalPath();
            System.out.println(msg);

        } catch (Exception ex) {
            localIDDatabase.abortTransaction(txn);
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public boolean verifyDb(File localIDFile)
            throws TException
    {
        boolean match = true;
        matchCnt = 0;
        matchFailCnt = 0;
        try {
            List<String[]> records = loadFile(localIDFile, 3);
            for (int i = 0; i < records.size(); i++) {
                String[] elements = (String[])records.get(i);
                String msg = "Elements:"
                        + " - e0=" + elements[0]
                        + " - e1=" + elements[1]
                        + " - e2=" + elements[2];
                if (elements[0].startsWith("***")) {
                    System.out.println("delete found - " + msg);
                    continue;
                }
                if (DEBUG) System.out.println("test -" + msg);
                boolean localmatchFound = matchLocalID(msg, elements[2], elements);
                boolean primarymatchFound = matchPrimaryID(msg, elements[2], elements);
                match = localmatchFound | primarymatchFound;
                if (DEBUG) System.out.println("Test"
                        + " - e0=" + elements[0]
                        + " - e1=" + elements[1]
                        + " - e2=" + elements[2]
                        + " - primarymatchFound=" + primarymatchFound
                        + " - localmatchFound=" + localmatchFound
                        );
                if (match) matchCnt++;
                else matchFailCnt++;
            }
            String msg = MESSAGE + "verifyDb"
                    + " - match=" + matchCnt
                    + " - fail=" + matchFailCnt
                    ;
            System.out.println(msg);
            if (matchFailCnt > 0) return false;
            else return true;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public boolean matchLocalID(String msg, String primaryID, String [] elements)
            throws TException
    {
        try {

            boolean matchFound = false;
               PrimaryIDStateInf primaryIDState
                        = localIDDatabase.getPrimaryIDState(elements[0], elements[1]);
                if (primaryIDState == null) {
                    System.out.println("primaryIDState null - " + msg);

                } else {
                    if (DEBUG) System.out.println(primaryIDState.dump("DUMP"));
                    primaryID = primaryIDState.getPrimaryIdentifier();
                    if (StringUtil.isEmpty(primaryID)) {
                        System.out.println("primaryID not found - " + msg);

                    } else if (!primaryID.equals(elements[2])) {
                        System.out.println("primaryID mismatch - " + msg
                                + " - primaryID=" + primaryID
                                );
                    } else matchFound = true;
                }
            return matchFound;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }


    public boolean matchPrimaryID(String line, String primaryID, String [] elements)
            throws TException
    {
        try {

            boolean matchFound = false;
            String msg = "Elements:"
                    + " - line=" + line
                    + " - e0=" + elements[0]
                    + " - e1=" + elements[1]
                    + " - e2=" + elements[2];
            LocalIDsState localIDsState = localIDDatabase.readPrimaryArrayDb(primaryID);
            if (localIDsState == null) {
                System.out.println("matchPrimaryID: localIDsState null - " + msg);

            } else {

                Vector<ContextLocalID> localList = localIDsState.getLocalIDs();
                if ((localList == null) || (localList.size() == 0)) {
                    System.out.println("matchPrimaryID: no LocaIDs found - " + msg);

                } else {
                    for (ContextLocalID id : localList) {
                        String localID = id.getLocalID();
                        String context = id.getContext();
                        if (context.equals(elements[0]) && localID.equals(elements[1])){
                            matchFound = true;
                            if (DEBUG) System.out.println("matchPrimaryID: match found " + msg);
                            break;
                        }
                    }
                }
            }
            return matchFound;

        } catch (Exception ex) {
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
                if (theLineArray[0].startsWith("***")) {
                    System.out.println("delete found - " + theLineArray[0]);
                    continue;
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

    public int getLoadCnt() {
        return loadCnt;
    }

    public int getMatchCnt() {
        return matchCnt;
    }

    public int getMatchFailCnt() {
        return matchFailCnt;
    }

    public LocalIDDatabase getLocalIDDatabase() {
        return localIDDatabase;
    }
}
