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
package org.cdlib.mrt.store.app.jersey;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.Vector;

/**
 * Process cleanup
 * @author dloy
 */
public class JerseyCleanup
        implements Closeable
{
    protected Vector<File> tempFiles = new Vector<File>(20);

    /**
     * temp files used during StorageService processing
     * Used for temp file cleanup
     * @param tempFile created temp file
     */
    public void addTempFile(File tempFile)
    {
        try {
            tempFiles.add(tempFile);

        } catch (Exception ex) {
            System.out.println("WARNING: Exception during addTempFile:" + ex);
        }
    }

    /**
     * Closable interface used by Jersey ClosableService
     */
    public void close()
    {
        deleteTemp();
    }

    /**
     * Return a list of temp files
     * Used for temp file cleanup
     * @return list of temp files
     */
    public void deleteTemp()
    {
        try {
            for (int i=0; i<tempFiles.size(); i++) {
                File file = tempFiles.get(i);
                if ((file == null) || !file.exists()) continue;
                try {
                    //System.out.println("!!!! JerseyCleanup - delete:" + file.getCanonicalPath());
                    file.delete();

                } catch (Exception ex) { }
            }
        } catch (Exception ex) {}
    }

    /**
     * Return a list of temp files
     * Used for temp file cleanup
     * @return list of temp files
     */
    public List<File> getTempFiles()
    {
        return tempFiles;
    }
}
