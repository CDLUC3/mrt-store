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

package org.cdlib.mrt.store;
import java.net.URL;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.store.NodeInf;

/**
 * StoreNode is a generic reference to a Storage node
 * This Class abstracts the reference to a specific Node which may be
 * either local or remote.
 * Local - a file reference can be made to the node
 * Remote - a URL is used to reference a storage node
 * @see org.cdlib.mrt.store.NodeInf
 * @see org.cdlib.mrt.store.can.CAN
 * @see org.cdlib.mrt.store.can.CANRemote
 *
 * @author dloy
 */
public class StoreNode
{

    protected NodeInf can = null;
    protected URL nodeLink = null;

    /**
     * Get base URL to remote node
     * @return base URL to remote node
     */
    public URL getNodeLink() {
        return nodeLink;
    }

    /**
     * Set base URL to remote node
     * @param nodeLink base URL to remote node
     */
    public void setNodeLink(URL nodeLink) {
        this.nodeLink = nodeLink;
    }

    /**
     * Get generic CAN reference (local or remote)
     * @return
     */
    public NodeInf getCan() {
        return can;
    }

    /**
     * Set generic CAN reference (local or remote)
     * @param can
     */
    public void setCan(NodeInf can) {
        this.can = can;
    }

    /**
     * Constructor
     * @param can generic CAN reference (local or remote)
     */
    public StoreNode(NodeInf can) {
        this.can = can;
    }
}
