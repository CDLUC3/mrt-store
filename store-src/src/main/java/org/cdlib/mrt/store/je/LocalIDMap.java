/*
 * THIS CLASS USED IN STORAGE LOCALID-OBJECTID JE SERIALIZATION
 * DO NOT MODIFY
 */
package org.cdlib.mrt.store.je;
import java.util.Date;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;

import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class LocalIDMap
{
    @SecondaryKey(relate=Relationship.MANY_TO_ONE)
    private String primaryID = null;
    @PrimaryKey
    private String contextLocalID = null;
    private String context = null;
    private String localID = null;
    private Date timestamp = null;

    public String getPrimaryID() {
        return primaryID;
    }

    public void setPrimaryID(String primaryID) {
        this.primaryID = primaryID;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getContextLocalID() {
        return contextLocalID;
    }

    public void setContextLocalID(String contextLocalID) {
        this.contextLocalID = contextLocalID;
    }

    public String getLocalID() {
        return localID;
    }

    public void setLocalID(String localID) {
        this.localID = localID;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = new Date(timestamp);
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

  

    public void setLocalID(String context, String localID, String objectID)
        throws TException
    {
        if (StringUtil.isEmpty(context) || StringUtil.isEmpty(localID)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "LocalID: setProfileLocalID argument is missing"
                    + " - profile=" + context
                    + " - localID=" + localID
                    );
        }
        if (StringUtil.isEmpty(objectID)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "LocalID: setProfileLocalID objectID is missing");
        }
        setContextLocalID(getID(context, localID));
        setContext (context);
        setLocalID (localID);
        setPrimaryID(objectID);
        setTimestamp(System.currentTimeMillis());
    }
    
    public static String getID(String profile, String localID)
    {
        return profile + "^^^" + localID;
    }

    public String dump(String header)
    {
        return header
                + " - contextLocalID=" + contextLocalID
                + " - context=" + context
                + " - localID=" + localID
                + " - primaryID=" + primaryID
                + " - timestamp=" + timestamp
                ;
    }
}
