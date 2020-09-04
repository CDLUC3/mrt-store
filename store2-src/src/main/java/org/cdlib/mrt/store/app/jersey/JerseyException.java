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

package org.cdlib.mrt.store.app.jersey;

import javax.ws.rs.core.Response;
import javax.ws.rs.WebApplicationException;

/**
 *
 * @author dloy
 */
public class JerseyException extends WebApplicationException
{
    //protected String msg = null;

    public JerseyException(Response.Status status, String message)
    {
        super(Response.ok(message, "text/plain").status(status).build());
    }
    
    
    JerseyException(Response.Status status)
    {
        super(Response.status(status).build());

    }

    public static class BAD_REQUEST  extends JerseyException
    {
        public BAD_REQUEST()
        {
            super(Response.Status.BAD_REQUEST);
        }
        public BAD_REQUEST(String msg)
        {
            super(Response.Status.BAD_REQUEST, msg);
        }
    }

    public static class INTERNAL_SERVER_ERROR   extends JerseyException
    {
        public INTERNAL_SERVER_ERROR ()
        {
            super(Response.Status.INTERNAL_SERVER_ERROR );
        }
        public INTERNAL_SERVER_ERROR (String msg)
        {
            super(Response.Status.INTERNAL_SERVER_ERROR , msg);
        }
    }

    public static class NOT_FOUND   extends JerseyException
    {
        public NOT_FOUND ()
        {
            super(Response.Status.NOT_FOUND );
        }
        public NOT_FOUND (String msg)
        {
            super(Response.Status.NOT_FOUND , msg);
        }
    }

    public static class UNAUTHORIZED    extends JerseyException
    {
        public UNAUTHORIZED  ()
        {
            super(Response.Status.UNAUTHORIZED  );
        }
        public UNAUTHORIZED  (String msg)
        {
            super(Response.Status.UNAUTHORIZED  , msg);
        }
    }

}
