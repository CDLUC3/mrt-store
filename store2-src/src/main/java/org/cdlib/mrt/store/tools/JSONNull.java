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

import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author dloy
 * Provides both the Presigned URL also returns any error that may have occurred during processing
 * 
 */
public class JSONNull
{
    protected JSONObject obj = null;
    
    public JSONNull(String jsonStr)
        throws Exception
    {
        obj = new JSONObject(jsonStr);
    }
    
    public String getString(String key)
    {
        try {
            return obj.getString(key);
        } catch (Exception ex) {
            return null;
        }
    }
    
    public Integer getInt(String key)
    {
        try {
            return obj.getInt(key);
        } catch (Exception ex) {
            return null;
        }
    }
    
    public Boolean getBoolean(String key)
    {
        try {
            return obj.getBoolean(key);
        } catch (Exception ex) {
            return null;
        }
    }
    
    public Long getLong(String key)
    {
        try {
            return obj.getLong(key);
        } catch (Exception ex) {
            return null;
        }
    }
    
    public ArrayList<String> getList(String key)
    {
        ArrayList<String> list = new ArrayList();
        JSONArray jsonArray= null;
        try {
            jsonArray= obj.getJSONArray(key);
        } catch (Exception ex) {
            return null;
        }
        for (int j=0; j < jsonArray.length(); j++) {
            try {
                String entry = (String)jsonArray.get(j);
                list.add(entry);
            } catch (Exception ex) {
                return null;
            }
        }
        return list;
    }
}