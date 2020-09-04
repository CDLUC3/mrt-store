/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store;

import java.io.File;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author replic
 */
public interface KeyFileInf {
    public void keyToFile (
                String key, File outFile)
        throws TException;
}
