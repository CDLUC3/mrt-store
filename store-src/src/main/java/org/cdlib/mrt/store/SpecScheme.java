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
import java.io.File;
import java.io.Serializable;

import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.StringUtil;

/**
 * SpecScheme contains enumerated values for Node properties
 * @author dloy
 */
public class SpecScheme
        implements Serializable
{

    protected final static String NL = System.getProperty("line.separator");
    protected final static boolean DEBUG = false;

    /**
     * Enumeration contains
     * type - category of SpecScheme
     * name - spec name
     * verion - version number for this spec
     */
    public enum Enum
    {
        access_1d0 ("access", "access", "1.0"),
        can_1d0 ("node", "can", "1.0"),
        clop_0d1 ("class", "clop", "0.1"),
        clop_0d3 ("class", "clop", "0.3"),
        clop_1d0 ("class", "clop", "1.0"),
        dflat_0d7 ("leaf", "dflat", "0.7"),
        dflat_0d17 ("leaf", "dflat", "0.17"),
        dflat_1d0 ("leaf", "dflat", "1.0"),
        checkm_0d1("manifest", "checkm", "0.1"),
        pairtree_0d1 ("branch", "pairtree", "0.1"),
        pairtree_1d0 ("branch", "pairtree", "1.0"),
        redd_1d0 ("delta", "redd", "1.0"),
        store_0d7 ("store", "store", "0.7"),
        store_1d0 ("store", "store", "1.0");

        protected final String type; // type of schema
        protected final String name; // name of product
        protected final String version; // version of product

        /**
         * Enumeration constructore
         * @param type category of SpecScheme
         * @param name spec name
         * @param version version number for this spec
         */
        Enum(String type, String name, String version) {
            this.type = type;
            this.name = name;
            this.version = version;
        }

        public String getType()   { return type; }
        public String getName()   { return name; }
        public String getVersion() { return version; }


        /**
         * get enumeration based on provided type, name and version
         * @param t type of spec
         * @param n name of spec
         * @param v version of spec
         * @return enumerated spec value
         */
        public static Enum valueOf(String t, String n, String v)
        {
            for (Enum p : Enum.values()) {
                if (p.getType().equals(t) && p.getName().equals(n) && p.getVersion().equals(v)) {
                    return p;
                }
            }
            return null;
        }
    }

    protected Enum scheme;
    protected String parameters = null;
    protected String specVersion = null;


    /**
     * General Constructor SpecScheme
     */
    public SpecScheme() { }


    /**
     * Constructor SpecScheme using
     * @param scheme spec enumerator
     * @param parameters string to be parsed for elements
     */
    public SpecScheme(SpecScheme.Enum scheme, String parameters)
    {
        this.scheme = scheme;
        this.parameters = parameters;
    }

    /**
     * Factory SpecScheme
     * @param type SpecScheme
     * @param line parameter list to be parsed
     * @return SpecScheme
     * @throws TException
     */
    public static SpecScheme buildSpecScheme(String type, String line)
            throws TException
    {
        SpecScheme specScheme = new SpecScheme();
        specScheme.parse(type, line);
        return specScheme;
    }

    /**
     * get current SpecScheme name
     * @return SpecScheme name
     */
    public String getName() {
        return scheme.getName();
    }

    /**
     * Get current SpecShceme version
     * @return version
     */
    public String getVersion(){
        return scheme.getVersion();
    }

    /**
     * Get SpecScheme enumerator
     * @return SpecScheme enumerator
     */
    public Enum getScheme()
    {
        return scheme;
    }

    /**
     * Set SpecScheme
     * @param type type of SpecScheme
     * @param name name of SpecScheme
     * @param version version of SpecScheme
     * @throws TException
     */
    public void setScheme(String type, String name, String version)
            throws TException
    {
        scheme = Enum.valueOf(type, name, version);
        if (scheme == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "SpecScheme not matched name=" + name
                    + " - version=" + version);
        }
    }


    /**
     * get unparsed parameter value
     * @return
     */
    public String getParameters() {
        return parameters;
    }

    /**
     * Set SpecScheme paramter
     * @param paramaters SpecScheme paramter
     */
    public void setParameters(String paramaters) {
        this.parameters = paramaters;
    }

    /**
     * Extract Name and Version from string value
     * Note that the matche name value is lower case, alphabetic string
     * @param line string parsed for Name, Version, and Parameters
     * @throws org.cdlib.mrt.utility.MException
     */
    public void parse(String type, String line)
            throws TException
    {
        try {
            if (StringUtil.isEmpty(line)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "SpecScheme - parm not provided");
            }
            if (StringUtil.isEmpty(type)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "SpecScheme - type not provided");
            }

            String[] items = line.split("\\s*\\/\\s*", 3);
            if ((items == null) || (items.length < 2)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        "SpecScheme - parm not valid - line=" + line);
            }
            String name = items[0].toLowerCase();
            name = StringUtil.stripNonAlphabetic(name);

            String version = null;
            if (items.length >= 2) {
                specVersion = items[1];
                version = "1.0";
            }
            if (items.length == 3) {
                String[] parts = items[2].split("\\s*\\;\\s*", 2);
                if (items == null) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                            "SpecScheme - parm not valid - line=" + line);
                }
                version = parts[0];
                parameters = null;
                if (parts.length > 1) parameters = parts[1];
            }

            //System.out.println("type=" + type + " - name=" + name + " - version=" + version);
            setScheme(type, name, version);
            setParameters(parameters);

        } catch (Exception ex) {
            if (DEBUG) {
                System.out.println("SpecScheme Exception:" + ex);
                System.out.println("SpecScheme trace:" + StringUtil.stackTrace(ex));
            }
            if (ex instanceof TException) {
                throw (TException) ex;
            }
            throw new TException.GENERAL_EXCEPTION("SpecScheme Exception:" + ex);
        }
    }

    /**
     * Get Namaste Name
     * @return
     */
    public String getNamasteName()
    {
        return "0=" + getName() + '_' + getVersion();
    }

    /**
     * Get content for Namaste file
     * @return
     */
    public String getNamasteContent()
    {
        return getFormatSpec();
    }

    @Override
    public String toString()
    {
        if (scheme == null) return null;
        String parm = "";
        String localSpecVersion = "";
        if (StringUtil.isNotEmpty(parameters)) parm = "; " + parameters;
        if (StringUtil.isNotEmpty(specVersion)) localSpecVersion = "/" + specVersion;
        return getName() + localSpecVersion + parm;
    }

    public String getReleaseVersion()
    {
        if (scheme == null) return null;
        return getVersion();
    }

 
    public String getFormatSpec()
    {
        if (scheme == null) return null;
        String name = toString();
        name = StringUtil.upperCaseFirst(name);
        return name;
    }

    public String getSpecVersion()
    {
        return specVersion;
    }

    /**
     * Build Namaste file
     * @param namasteFile Namaste file to build
     * @throws TException process exception
     */
    public void buildNamasteFile(File namasteDirectory)
            throws TException
    {
        if (DEBUG) {
            try {
                System.out.println("SpecScheme: buildNamasteFile"
                        + " - namasteDirectory=" + namasteDirectory.getCanonicalPath()
                        + " - scheme=" + scheme.toString()
                        );
            } catch (Exception ex) {}
        }
        String namasteName = getNamasteName();
        try {
            File namaste = new File(namasteDirectory, namasteName);
            if (DEBUG) {
                System.out.println("SpecScheme: buildNamasteFile"
                        + " - namasteName=" + namasteName
                        + " - exists=" + namaste.exists()
                        );
            }
            if (namaste.exists()) return;
            String content = getNamasteContent();
            FileUtil.string2File(namaste, content + NL);

        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("SpecScheme: buildNamasteFile fails:" + ex);
        }
    }

}
