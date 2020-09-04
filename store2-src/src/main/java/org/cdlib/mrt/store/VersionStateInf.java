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

import java.io.Serializable;
import java.net.URL;
import java.util.List;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.LinkedHashList;

/**
 * Version State information
 * @author dloy
 */
public interface VersionStateInf
        extends StateInf
{
    /**
     * Get object reference
     * @return object reference
     */
    public URL getObjectState();

    /**
     * Get this version reference
     * @return version reference
     */
    public URL getVersion();

    /**
     * Get version identifier
     * @return version identifier
     */
    public int getIdentifier();

    /**
     * Get list of fileState URLs
     * @return fileState URLs
     */
    public LinkedHashList<String, String> getFileStates();

    /**
     * Get creation date-time version
     * @return
     */
    public DateState getCreated();

    /**
     * Get number of files in Version
     * @return number of files in Version
     */
    public int getNumFiles();

    /**
     * Get number of bytes for files in Version
     * @return total number of bytes
     */
    public long getTotalSize();

    /**
     * Does this version state refer to the current version
     * @return true=current version, false=not current version
     */
    public boolean isCurrent() ;

    /**
     * Change in number of Object files
     * @return change in the number of files in this Object
     */
    public Long getDeltaNumFiles();

    /**
     * Chang in the size of this Object
     * @return change in the size of this Object
     */
    public Long getDeltaSize();

    /**
     * Number of files not specifically components
     * @return number of files
     */
    public Long getNumActualFiles();

    /**
     * Number of bytes not specifically components
     * @return number of bytes
     */
    public Long getTotalActualSize();
    
    /**
     * For virtual node return the physical node
     * @return  physical node
     */
    public Integer getPhysicalNode();
}
