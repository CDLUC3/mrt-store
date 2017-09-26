/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.je;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 *
 * @author dloy
 */
public class DataAccessor
{
    // Open the indices
    public DataAccessor(EntityStore store)
        throws DatabaseException {

        // Primary key for Inventory classes
        primaryIndex = store.getPrimaryIndex(
            String.class, LocalIDMap.class);

         // Secondary key for SimpleEntityClass classes
        // Last field in the getSecondaryIndex() method must be
        // the name of a class member; in this case, an
        // SimpleEntityClass.class data member.
        secondaryIndex = store.getSecondaryIndex(
            primaryIndex, String.class, "primaryID");

    }

    // Inventory Accessors
    PrimaryIndex<String,LocalIDMap> primaryIndex;
    SecondaryIndex<String,String,LocalIDMap> secondaryIndex;


}
