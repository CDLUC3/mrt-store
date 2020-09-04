/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.can;
import org.cdlib.mrt.utility.StringUtil;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author dloy
 */
public class CANTest {

    public CANTest() {
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



    @Test
    public void testGetLocalIDs()
    {

        try {
            validate(null, null);
            validate("", null);
            validate("AAAA", new  String [] {"AAAA"});
            validate("AAAA;BBBBB", new  String [] {"AAAA","BBBBB"});
            validate(" AAAA;BBBBB ", new  String [] {"AAAA","BBBBB"});
            validate(" AAAA ; BBBBB ", new  String [] {"AAAA","BBBBB"});

        } catch (Exception ex) {
            assertFalse("Exception:" + ex, true);
        }
    }

    protected void validate(String in, String [] listTest)
    {
        List<String> arr = CANCloud.getLocalIDs(in);
        dump(in, arr, listTest);
        if ((listTest == null)) {

            assertTrue("bad null return:",
                    arr == null);
            return;
        }
        if (listTest == null) {
            assertTrue(true);
        }
        if (arr.size() != listTest.length) {
            assertTrue("mismatch validation size:"
                    + " - arr.size=" + arr.size()
                    + " - listTest.length=" + listTest.length,
                    false);
        }
        for (int i=0; i < arr.size(); i++) {
            String val1 = arr.get(i);
            String val2 = listTest[i];
            assertTrue("mismatch:"
                    + " - val1=" + val1
                    + " - val2=" + val2
                    ,val1.equals(val2));
        }
    }

    protected void dump(String in, List<String> arr, String [] listTest) {
        System.out.println("**Dump: \"" + in + "\"");
        boolean ret = false;
        if (arr == null) {
            System.out.println("arr= NULL");
            ret = true;
        } else {
            System.out.println("arr.size()=" + arr.size());
        }
        if (listTest == null) {
            System.out.println("listTest= NULL");
            ret = true;
        } else {
            System.out.println("listTest.length=" + listTest.length);
        }
        if (ret) return;
        if (arr.size() != listTest.length) {
            System.out.println("Length mismatch:"
                    + " - arr.size()=" + arr.size()
                    + " - listTest.length=" + listTest.length
                    );
            return;
        }
        for (int i=0; i < arr.size(); i++) {
            String val1 = arr.get(i);
            String val2 = listTest[i];
            System.out.println("arr:\"" + val1 + "\" - list:\"" + val2 + "\"");
        }
    }

}