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
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TRuntimeException;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author dloy
 * Provides both the Presigned URL also returns any error that may have occurred during processing
 * 
 */
public class TokenPostState
        implements StateInf, Serializable
{
    protected static final String NAME = "TokenPostState";
    protected static final String MESSAGE = NAME + ": ";

    protected TokenStatus tokenStatus = null;
    protected RunStatus runStatus = null;
    protected Exception ex = null;

    // Presigned errors
    public enum RunStatus
    {
        OK("Request queued, use token to check status", 200),
        
        NotReady("Object is not ready", 202),
        
        REQUESTED_ITEM_NOT_FOUND("Object content not found", 404),

        FORBIDDEN_OPERATION("Forbidden operation", 403),

        SERVICE_EXCEPTION("Service exception", 500);

        protected final String description;
        protected final int httpResponse;

        RunStatus(String description, int httpResponse) {
            this.description = description;
            this.httpResponse = httpResponse;
        }
        public String getDescription()
        {
            return description;
        }
        public int getHttpResponse()
        {
            return httpResponse;
        }
        public String dump(String header)
        {
            return header + " - httpResponse:" + httpResponse + " - description:" + description;
        }
    }
    
    public static TokenPostState getTokenPostState(TokenStatus tokenStatus)
        throws TException
    {
        return new TokenPostState(tokenStatus);
    }
    
    protected TokenPostState(TokenStatus tokenStatus)
        throws TException
    {
        this.tokenStatus = tokenStatus;
        setRunStusFromToken();
    }
    
    public static TokenPostState getTokenPostState(Exception ex)
        throws TException
    {
        return new TokenPostState(ex);
    } 
    
    protected TokenPostState(Exception ex)
        throws TException
    {
        this.setEx(ex);
        if (ex.toString().contains("404")) {
            runStatus = RunStatus.REQUESTED_ITEM_NOT_FOUND;
            
        } else if (ex instanceof TException.REQUEST_ITEM_EXISTS) {
            runStatus = RunStatus.FORBIDDEN_OPERATION;
            
        } else {
            runStatus = RunStatus.SERVICE_EXCEPTION;
        }
    }

    private void setRunStusFromToken()
        throws TException
    {
        switch (tokenStatus.getTokenStatusEnum()) {
            case OK:
                runStatus = RunStatus.OK;
                break;
            case NotReady:
                runStatus = RunStatus.OK;
                break;
            case SERVICE_EXCEPTION:
                runStatus = runStatus.SERVICE_EXCEPTION;
                break;
        }
            
    }
    public String getToken() {
        return tokenStatus.getToken();
    }

    public DateState getAnticipatedAvailableDate() {
        return tokenStatus.getAnticipatedAvailableDate();
    }

    public Long getCloudContentBytes() {
        return tokenStatus.getCloudContentBytes();
    }

    public TokenStatus getTokenStatus() {
        return tokenStatus;
    }

    public void setTokenStatus(TokenStatus tokenStatus) {
        this.tokenStatus = tokenStatus;
    }

    public RunStatus getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(RunStatus status) {
        this.runStatus = status;
    }

    public int getHttpStatus() {
        return runStatus.getHttpResponse();
    }

    /**
     * get Exception if not controlled by ExceptionEnum
     * @return 
     */
    public Exception getEx() {
        return ex;
    }

    public void setEx(Exception ex) {
        this.ex = ex;
    }
    /**
     * Return the Json value of this TplemStatus
     * @return JSON for TokenStatus
     */
    public String getJsonOK()
    {
        //JsonObject tokenJson = new JsonObject();
        try {
            JSONObject tokenJson = new JSONObject();
            tokenJson.put("status", RunStatus.OK.getHttpResponse());
            tokenJson.put("token", getToken());
            tokenJson.put("cloud-content-byte", getCloudContentBytes());
            tokenJson.put("anticipated-availability-time", getAnticipatedAvailableDate().getIsoDate());
            tokenJson.put("message", RunStatus.OK.getDescription());
            return tokenJson.toString();
            
        } catch (Exception ex) {
            System.out.println("TokenStatus Exception:" + ex);
            return null;
        }
    }
    
    /**
     * Return the Json value of this TplemStatus
     * @return JSON for TokenStatus
     */
    public String getJsonError()
    {
        //JsonObject tokenJson = new JsonObject();
        try {
            String errMessage = runStatus.getDescription();
            if (runStatus.getHttpResponse() == 500) {
                errMessage = ex.toString();
            }
            JSONObject tokenJson = new JSONObject();
            tokenJson.put("status", runStatus.getHttpResponse());
            tokenJson.put("message", errMessage);
            return tokenJson.toString();
            
        } catch (Exception ex) {
            System.out.println("TokenStatus Exception:" + ex);
            return null;
        }
    }
}

