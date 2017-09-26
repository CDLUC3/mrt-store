/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.storage;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.store.InfoText;

/**
 * Define property values for setting SpecScheme enums
 * @author dloy
 */
public class StorageInfo
    extends InfoText
{
    public static final String SERVICECHEME = "Service-scheme";
    public static final String NODESCHEME = "Node-scheme";

    /**
     * Constructor
     * @param loadString Properties file as String
     * @throws TException process exception
     */
    public StorageInfo(String loadString)
        throws TException
    {
        loadInfoText(loadString);
    }

}
