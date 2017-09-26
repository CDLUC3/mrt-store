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

package org.cdlib.mrt.store.tools;

import org.cdlib.mrt.utility.URLEncoder;
import java.net.URL;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Utility routines for the Storage service
 * @author dloy
 */
public class StoreUtil
{
    public static final boolean debug = false;
    
    public static URL buildContentURL(
            String type,
            URL accessURL,
            Integer nodeID,
            Identifier objectID,
            Integer versionID,
            String fileName
            )
    {
        try {
            if (accessURL == null) return null;
            StringBuffer buf = new StringBuffer();
            if (debug) {
                System.out.println("!!!!StoreUtil.buildContentURL:" 
                        + " - type=" + type
                        + " - accessURL=" + accessURL
                        + " - nodeID=" + nodeID
                        + " - objectID=" + objectID
                        + " - versionID=" + versionID
                        + " - fileName=" + fileName
                        );
            }
            buf.append(accessURL.toString());
            if (StringUtil.isNotEmpty(type)) buf.append("/" + type);
            if (nodeID != null) buf.append("/" + nodeID);
            if (objectID != null) buf.append("/" + URLEncoder.encode(objectID.getValue(), "utf-8"));
            if (versionID != null) buf.append("/" + versionID);
            if (StringUtil.isNotEmpty(fileName)) buf.append("/" + URLEncoder.encode(fileName, "utf-8"));
            URL retURL = new URL(buf.toString());

            if (debug)
                System.out.println("!!!!StoreUtil.buildContentURL:"
                        + " - return=" + retURL
                        );
            return retURL;

        } catch (Exception ex) {
            System.out.println("StoreUtil.buildContentURL Exception:" + ex);
            return null;
        }
    }
}
