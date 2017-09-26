/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.dflat;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.store.InfoText;

/**
 * Define property values for setting SpecScheme enums
 * @author dloy
 */
public class DflatInfo
    extends InfoText
{
    public static final String FULLSCHEME = "fullScheme";
    public static final String DELTASCHEME = "deltaScheme";
    public static final String MANIFESTSCHEME = "manifestScheme";
    public static final String CURRENTSCHEME = "currentScheme";
    public static final String CLASSSCHEME = "classScheme";
    public static final String LEAFSCHEME = "objectScheme";
    
    /**
     * Constructor
     * @param loadString Properties file as String
     * @throws TException process exception
     */
    public DflatInfo(String loadString)
        throws TException
    {
        loadInfoText(loadString);
    }

}
