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
**********************************************************/
package org.cdlib.mrt.store;
import java.io.InputStream;
import java.util.Properties;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Generalized SpecScheme resolver
 * Data is input in a properties file type format.
 * The constructed Properties is then accessed for a value that is converted
 *  into a SpecScheme enum
 * @author dloy
 */
public class InfoText
{
    protected Properties infoProp = new Properties();

    /**
     * Load a set of properties saved as a String with LF and standard File properties format
     * @param loadString String equivalent of a properties load file format
     * @throws TException
     */
    public void loadInfoText(String loadString)
        throws TException
    {
        try {
            if (StringUtil.isEmpty(loadString)) return;
            InputStream stream = StringUtil.stringToStream(loadString, "utf-8");
            infoProp.load(stream);
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION(
                    "InfoText: Exception:" + ex);
        }
    }

    /**
     * Get a SpecScheme enum value base off of SpecShcme type and key
     * @param type SpecScheme enum type (leaf,manifest,branch,delta,store);
     * @param key SpecScheme key
     * @return SpecScheme enum
     * @throws TException
     */
    public SpecScheme getSpecScheme(String type, String key)
        throws TException
    {
        if (StringUtil.isEmpty(key)) return null;
        if (StringUtil.isEmpty(type)) return null;
        String value = infoProp.getProperty(key);
        //System.out.println("getSpecScheme key=" + key + " - type=" + type + " - value=" + value);
        //System.out.println(PropertiesUtil.dumpProperties("getSpecScheme", infoProp));
        if (StringUtil.isEmpty(value)) return null;
        return SpecScheme.buildSpecScheme(type, value);
    }

    /**
     * Build properties string
     * @return Properties String
     */
    public String getLoadProperties()
    {
        return PropertiesUtil.buildLoadProperties(infoProp);
    }

    /**
     * Get property
     * @param key property key
     * @return property value
     */
    public String getProperty(String key)
    {
        if (StringUtil.isEmpty(key)) return null;
        return infoProp.getProperty(key);
    }

    /**
     * Set property value
     * @param key property key
     * @param value property value
     */
    public void setProperty(String key, String value)
    {
        if (StringUtil.isEmpty(key)) return;
        if (StringUtil.isEmpty(value)) return;
        infoProp.setProperty(key, value);
    }
}
