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

import java.util.List;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.LinkedHashList;

/**
 *
 * @author dloy
 */
public interface StorageServiceStateInf
{

    public static final String SERVICENAME = "name";
    public static final String SERVICEID = "identifier";
    public static final String SERVICEDESCRIPTION = "description";
    public static final String SERVICESCHEME = "serviceScheme";
    public static final String BASEURI = "baseURI";
    public static final String SUPPORTURI = "supportURI";

    /**
     * Get Storage Service Name
     * @return Storage Service Name
     */
    public String getName();

    /**
     * Get Storage Service identifier
     * @return Storage Service identifier
     */
    public String getIdentifier();

    /**
     * Get Storage Service description
     * @return Storage Service description
     */
    public String getDescription();

    /**
     * Get Service Scheme
     * @return Service Scheme
     */
    public String getServiceScheme();

    /**
     * Get Implementation Version
     * @return implementation
     */
    public String getVersion();

    /**
     * Get Access URI
     * @return Access URI
     */
    public String getBaseURI();

    /**
     * Get Support URI
     * @return Support URI
     */
    public String getSupportURI();

    /**
     * Get list of Nodes in service
     * @return list of Nodes references
     */
    public LinkedHashList<String, String> getNodeStates();

    /**
     * Get approximate number of files in Node
     * @return approximate number of files
     */
    public long getNumFiles();

    /**
     * Get approximate number of Objects within Node
     * @return approximate number of Objects
     */
    public long getNumObjects();

    /**
     * Get approximate number of Version within Node
     * @return approximate number of version
     */
    public long getNumVersions();

    /**
     * Last Access date-time to service
     * Not technically feasible
     * @return Access date-time
     */
    public DateState getAccessDateTime();

    /**
     * Get creation date-time for this Node
     * Corresponds to the date-time of the node directory file
     * @return creation date-time for this Node
     */
    public DateState getCreated();

    /**
     * Get last update date-time for this Node.
     * Corresponds to the date-time for last update to the node counter file
     * log/summary-stats.txt
     * @returnlast update date-time
     */
    public DateState getUpdateDateTime();

    /**
     * Commands supported by service
     * @return String of supported commands
     */
    public String getCommands();

    /**
     * Number of files - not specifically components
     * @return number of files
     */
    public long getNumActualFiles();
    /**
     * get the number of files - not specifically components
     * @return number of files
     */
    public long getTotalActualSize() ;

    /**
     * Dump of Service State dump information
     * @param header dump header
     * @return service State dump
     */
    public String dump(String header);

}

