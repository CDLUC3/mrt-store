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
import java.util.Set;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.utility.StateInf;

/**
 * Information about a specific file
 * Note that not all of these values are generally set in response
 * @author dloy
 */
public interface FileStateInf
        extends StateInf
{
    /**
     * Name of file including relative path values
     * @return File name
     */
    public String getIdentifier();

    /**
     * Size of file in bytes
     * @return file size
     */
    public long getSize();

    /**
     * Return the first Message digest in digest set
     * Typically there will only be one as saved in manifest
     * @return first (or only) message digest
     */
    public MessageDigest getMessageDigest();

    /**
     * Last modified Date
     * @return last modified date
     */
    public DateState getCreated();

    /**
     * URL reference to file
     * @return file reference
     */
    public URL getURL();

    /**
     * set of MessageDigest - each containing a unique MessageDigest type (e.g. checksum type)
     * @return Set of message digests for this specific file
     */
    public Set<MessageDigest> getMessageDigests();

    /**
     * File Mime type
     * @return file Mime Type
     */
    public String getMimeType();

    /**
     * Return primaryID
     * @return primaryID
     */
    public String getPrimaryID();

    /**
     * Return localID
     * @return localID
     */
    public String getLocalID();

    /**
     * get reference to file content
     * @return reference to file content
     */
    public URL getFile();

    /**
     * get reference to version state
     * @return version state
     */
    public URL getVersionState();
}
