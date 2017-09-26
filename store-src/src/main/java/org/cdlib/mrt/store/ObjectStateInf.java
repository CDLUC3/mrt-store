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

import java.net.URL;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StateInf;

/**
 * State information for Object
 * @author dloy
 */
public interface ObjectStateInf
        extends StateInf
{


    /**
     * Get Object identifier
     * @return Object identifier
     */
    public Identifier getIdentifier();

    /**
     * Get total number files in Object
     * @return number files
     */
    public int getNumFiles();

    /**
     * Get total number versions in Object
     * @return number versions
     */
    public int getNumVersions();

    /**
     * Get byte size for Object
     * @return number bytes
     */
    public long getSize();

    /**
     * Get byte size for Object
     * @return number bytes
     */
    public Long getNumActualFiles();

    /**
     * Get byte size for Object
     * @return number bytes
     */
    public Long getTotalActualSize();

    /**
     * get reference to Object content
     * @return reference to Object content
     */
    public URL getObject();

    /**
     * get reference to current Version
     * @return reference to current Version
     */
    public URL getCurrentVersionState();

    /**
     * get reference to node state
     * @return reference to node state
     */
    public URL getNodeState();

    /**
     * Date of last added version
     * @return date last added version
     */
    public DateState getLastAddVersion();

    /**
     * Date of last deleted version
     * @return date last deleted version
     */
    public DateState getLastDeleteVersion();

    /**
     * Date of last fixity
     * @return date last fixity
     */
    public DateState getLastFixity();

    /**
     * Get list of VersionState values
     * @return
     */
    public LinkedHashList<String, String> getVersionStates();

    /**
     * Get access context (profile)
     * @return access context
     */
    public String getContext();

    /**
     * Get local identifier
     * @return local identifier
     */
    public String getLocalID();

    /**
     * DeleteID state from a deleteObject
     * @return 
     */
    public DeleteIDState getDeleteIDState();
    
    /**
     * @return node identifier
     */
    public int getNodeID();
    
    /**
     * For virtual node return the physical node
     * @return  physical node
     */
    public Integer getPhysicalNode();
}
