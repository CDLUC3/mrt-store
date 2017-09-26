/*
Copyright (c) 2005-2010, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*********************************************************************/

package org.cdlib.mrt.store;
import java.io.File;
import java.util.Date;
import java.util.Properties;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Handle management of last-activity.txt
 * @author dloy
 */
public class LastActivity
{

    public enum Type {
        LASTFIXITY("lastFixity"),
        LASTADDVERSION("lastAddVersion"),
        LASTDELETEVERSION("lastDeleteVersion"),
        LASTDELETEOBJECT("lastDeleteObject");

        protected final String key; // key value

        /**
         * Enumeration constructore
         * @param type category of SpecScheme
         * @param name spec name
         * @param version version number for this spec
         */
        Type(String key) {
            this.key = key;
        }

        public String getKey()   { return key; }
    }

    protected Properties activityProp = null;
    protected File activityFile = null;

    public LastActivity(File activityFile)
        throws TException
    {
        if (activityFile == null) {
            throw new TException.INVALID_OR_MISSING_PARM("LastActivity: file required");
        }
        if (activityFile.exists()) {
            this.activityProp = PropertiesUtil.loadFileProperties(activityFile);
        } else {
            activityProp = new Properties();
        }

        this.activityFile = activityFile;
    }

    public void setPropery(Type key)
        throws TException
    {
        if (key == null) {
            throw new TException.INVALID_OR_MISSING_PARM("LastActivity.setProperty: key not set");
        }
        String keyS = key.getKey();
        String dateS = DateUtil.getCurrentIsoDate();
        activityProp.setProperty(keyS, dateS);
    }

    /**
     * Get date if exists
     * @param key for last activity date
     * @return date
     * @throws TException
     */
    public DateState getDate(Type key)
        throws TException
    {

        if (key == null) {
            throw new TException.INVALID_OR_MISSING_PARM("LastActivity.setProperty: key not set");
        }
        String keyS = key.getKey();
        String dateS = activityProp.getProperty(keyS);
        Date date = DateUtil.getIsoDateFromString(dateS);
        if (date == null) return null;
        return new DateState(date);
    }

    /**
     * Write file with content from properties
     * @throws TException
     */
    public void writeFile()
        throws TException
    {
        if (activityProp.size() == 0) {
            throw new TException.INVALID_OR_MISSING_PARM("LastActivity.writeFile: no property values set");
        }
        String outProp = PropertiesUtil.buildLoadProperties(activityProp);
        FileUtil.string2File(activityFile, outProp);
    }
}
