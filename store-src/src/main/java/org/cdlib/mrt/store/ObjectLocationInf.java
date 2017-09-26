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
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author dloy
 */
public interface ObjectLocationInf{

    /**
     * Create a Location directory using this identifier
     * @param id identifier for generation of location File
     * @return existing directory File
     * @throws org.cdlib.mrt.utility.MException
     */
    public File buildObjectLocation (Identifier id)
        throws TException;

    /**
     * Create a File using this identifier - this file may not exist
     * @param id identifier for generation of location File
     * @return existing or non-existing directory File
     * @throws org.cdlib.mrt.utility.MException
     */
    public File getObjectLocation (Identifier id)
        throws TException;

    /**
     * Test if file exists at this location
      * @param id identifier for generation of location File
     * @return true=object exists - false=object does not exist
     * @throws org.cdlib.mrt.utility.MException
     */
    public boolean objectExists (Identifier id)
        throws TException;

    /**
     * Remove base directory and all empty parent directories
     * @param id identifier for beginning of directory removal
     * @return true=all directories removed; false=at least one directory not deleted
     * @throws org.cdlib.mrt.utility.MException
     */
    public boolean removeObjectLocation (Identifier id)
            throws TException;

}

