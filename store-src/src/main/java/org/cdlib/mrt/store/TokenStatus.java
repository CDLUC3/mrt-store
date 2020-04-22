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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
//import com.google.gson.Gson;
//import com.google.gson.JsonObject;
//import com.google.gson.GsonBuilder;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.ArchiveBuilder;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TRuntimeException;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.store.tools.JSONNull;
import org.json.*;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author dloy
 * TokenStatus contains all properties needed to run async container building
 * 
 */
public class TokenStatus
        implements StateInf, Serializable
{
    // object-object container, version-version container, producer-version container, without system content
    public enum ArchiveContent {object, version, producer} 
    
    // Current completion status - OK=exists, NotReady=in process, SERVICE_EXCEPTION=build and store failed
    public enum TokenStatusEnum
    {
        OK("Container complete", 200),
        
        NotReady("Object is not ready", 202),

        SERVICE_EXCEPTION("Service exception", 500);

        protected final String description;
        protected final int httpResponse;
        

        TokenStatusEnum(String description, int httpResponse) {
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
    
    protected static final String NAME = "TokenStatus";
    protected static final String MESSAGE = NAME + ": ";

    protected String nodeIOName = null;
    protected Long extractNode = null;
    protected Long deliveryNode = null;
    protected Identifier objectID = null;
    protected Integer versionID = null;
    protected DateState anticipatedAvailableDate = null;
    protected Date buildStart = null;
    protected Date buildEnd = null;
    protected Long cloudContentBytes = null;
    protected String token = null;
    protected Integer exStatus = null;
    protected String exMsg = null;
    protected Boolean fullObject = null;
    protected Boolean returnOnError = null;
    protected ArchiveContent archiveContent = null;
    protected ArchiveBuilder.ArchiveType archiveType = null;
    protected TokenStatusEnum tokenStatusEnum = TokenStatusEnum.NotReady;
    protected List<String> filterList = null;
    
    public static void main(String[] args) 
        throws TException
    {
        TokenStatus tokenStatus = TokenStatus.getTokenStatus();
        try {
            tokenStatus.currentBuildStart();
            tokenStatus.setNodeIOName("nodes-stage");
            tokenStatus.setExtractNode(9502L);
            tokenStatus.setDeliveryNode(7001L);
            tokenStatus.setObjectID("ark:/28722/bk0006w8m0c");
            tokenStatus.setVersionID(2);
            long millSecs = 3 * 60 * 60 * 1000;
            Date date =  DateUtil.getCurrentDatePlus(millSecs);
            tokenStatus.setAnticipatedAvailableDate(new DateState(date));
            tokenStatus.setCloudContentBytes(123456789L);
            tokenStatus.setToken("245f46bf-0e37-4577-ba45-9ac8c71fa4bd");
            tokenStatus.setFullObject(true);
            tokenStatus.setReturnOnError(false);
            tokenStatus.setArchiveType(ArchiveBuilder.ArchiveType.targz);
            tokenStatus.setArchiveContent(ArchiveContent.producer);
            tokenStatus.currentBuildStart();
            ArrayList<String> filterList = new ArrayList();
            filterList.add("a");
            filterList.add("bb");
            filterList.add("ccc");
            tokenStatus.setFilterList(filterList);
            tokenStatus.currentBuildEnd();
            System.out.println("Archive mime:" + tokenStatus.getArchiveTypeMime());
            
            String jsonOut=tokenStatus.getJson();
            System.out.println("JSONOUT:" + jsonOut);
            System.out.println("anticipatedAvailableDate:" + tokenStatus.getAnticipatedAvailableDate().getIsoDate());
            TokenStatus tokenStatus2 = TokenStatus.getTokenStatusFromJson(jsonOut);
            System.out.println(tokenStatus2.dump("TOKENSTATUS2"));
            

        } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }

    /**
     * @return new PreSignedState without content
     * @throws TException 
     */
    public static TokenStatus getTokenStatus()
        throws TException
    { 
        return new TokenStatus();
    }
    
    /**
     * constructor empty state
     * @throws TException 
     */
    protected TokenStatus()
        throws TException
    {  }

    /**
     * Approximate date-time for completion of container build
     * @return existing anticipated date-time
     */
    public DateState getAnticipatedAvailableDate() {
        return anticipatedAvailableDate;
    }
    
    /**
     * set Anticipated date of container creation
     * @param anticipatedAvailableDate 
     */
    public void setAnticipatedAvailableDate(DateState anticipatedAvailableDate) {
        this.anticipatedAvailableDate = anticipatedAvailableDate;
    }
    /**
     * 
     * @return existing Anticipated Date in seconds
     */
    public Long getApproximateCompletionSeconds() {
        if (anticipatedAvailableDate == null) {
            return null;
        }
        Date date = anticipatedAvailableDate.getDate();
        Long seconds = date.getTime();
        return seconds;
    }
    
    public void buildApproximateCompletionDate(long addSeconds) {
        anticipatedAvailableDate = new DateState();
        long millSecs = addSeconds * 1000;
        Date date =  DateUtil.getCurrentDatePlus(millSecs);
        anticipatedAvailableDate.setDate(date);
    }
    
    /**
     * 
     * @return existing Anticipated Date in seconds
     */
    public void setApproximateCompletionDateSeconds(long seconds) {        
        if (seconds <= 0) {
            anticipatedAvailableDate = null;
            return;
        }
        Date date = new Date(seconds);
        anticipatedAvailableDate = new DateState();
        anticipatedAvailableDate.setDate(date);
    }

    public void setApproximateCompletionDateSeconds(String secondsS) {
        Long seconds = null;
        try {
            seconds = Long.parseLong(secondsS);
        } catch (Exception ex) {
            seconds = null;
        }
        setApproximateCompletionDateSeconds(seconds);
    }
    
    /**
     * @return calculated number of bytes for building container
     */
    public Long getCloudContentBytes() {
        return cloudContentBytes;
    }

    /**
     * Set n8umber of bytes for building container
     * @param cloudContentBytes 
     */
    public void setCloudContentBytes(Long cloudContentBytes) {
        this.cloudContentBytes = cloudContentBytes;
    }

    /**
     * @return Token used for polling
     */
    public String getToken() {
        return token;
    }

    /**
     * Set token used for polling
     * @param token 
     */
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * NodeIO table name for the provided nodes in this TokenStatus
     * @return 
     */
    public String getNodeIOName() {
        return nodeIOName;
    }

    public void setNodeIOName(String nodeIOName) {
        this.nodeIOName = nodeIOName;
    }

    /**
     * Node number as used in nodeIo Name table for extracting container content
     * @return 
     */
    public Long getExtractNode() {
        return extractNode;
    }

    public void setExtractNode(Long extractNode) {
        this.extractNode = extractNode;
    }

    /**
     * Node number as used in nodeIo Name table for saving container
     * @return 
     */
    public Long getDeliveryNode() {
        return deliveryNode;
    }

    public void setDeliveryNode(Long deliveryNode) {
        this.deliveryNode = deliveryNode;
    }

    /**
     * Object of container extraction
     * @return 
     */
    public Identifier getObjectID() {
        return objectID;
    }

    public String getObjectIDS() {
        if (objectID == null) {
            return null;
        }
        return objectID.getValue();
    }

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    public void setObjectID(String objectIDS) {
        try {
            this.objectID = new Identifier(objectIDS);
        } catch (Exception ex) {
            this.objectID = null;
        }
    }

    /**
     * Version number for Container extraction if required - 0=current
     * @return 
     */
    public Integer getVersionID() {
        return versionID;
    }

    public void setVersionID(Integer versionID) {
        this.versionID = versionID;
    }

    public Boolean getFullObject() {
        return fullObject;
    }

    /**
     * Flag - true=duplicate files in versions even if not delta
     * @param fullObject 
     */
    public void setFullObject(Boolean fullObject) {
        this.fullObject = fullObject;
    }

    public Boolean getReturnOnError() {
        return returnOnError;
    }

    public void setReturnOnError(Boolean returnOnError) {
        this.returnOnError = returnOnError;
    }

    /**
     * HTTP status for building container file and save
     * @return 
     */
    public Integer getExStatus() {
        return exStatus;
    }

    public void setExStatus(Integer exStatus) {
        this.exStatus = exStatus;
    }

    /**
     * Exception message for building container file
     * @return 
     */
    public String getExMsg() {
        return exMsg;
    }

    public void setExMsg(String exMsg) {
        this.exMsg = exMsg;
    }

    /**
     * Current status for building and preserving container
     * @return 
     */
    public TokenStatusEnum getTokenStatusEnum() {
        return tokenStatusEnum;
    }

    public void setTokenStatusEnum(TokenStatusEnum tokenStatusEnum) {
        this.tokenStatusEnum = tokenStatusEnum;
    }

    public ArchiveContent getArchiveContent() {
        return archiveContent;
    }

    public void setArchiveContent(String archiveContentS) {
        if (archiveContentS == null) {
            return;
        }
        this.archiveContent = ArchiveContent.valueOf(archiveContentS);
    }

    public void setArchiveContent(ArchiveContent archiveContent) {
        this.archiveContent = archiveContent;
    }

    public ArchiveBuilder.ArchiveType getArchiveType() {
        return archiveType;
    }

    public void setArchiveType(ArchiveBuilder.ArchiveType archiveType) {
        this.archiveType = archiveType;
    }

    public void setArchiveType(String archiveTypeS) {
        if (archiveTypeS == null) {
            return;
        }
        this.archiveType = ArchiveBuilder.ArchiveType.valueOf(archiveTypeS);
    }

    public void setTokenStatusEnum(String tokenStatusEnumS) {
        if (tokenStatusEnumS == null) {
            return;
        }
        this.tokenStatusEnum = TokenStatusEnum.valueOf(tokenStatusEnumS);
    }

    public List<String> getFilterList() {
        return filterList;
    }

    public void setFilterList(List<String> filterList) {
        this.filterList = filterList;
    }

    public String getArchiveTypeMime() {
        return this.archiveType.getMimeType();
    }

    public Date getBuildStart() {
        return buildStart;
    }
    
    public Long getBuildStartMil() {
        if (buildStart == null) return null;
        return buildStart.getTime();
    }

    public void setBuildStart(Date buildStart) {
        this.buildStart = buildStart;
    }

    public void setBuildStart(Long buildStartMil) {
        if (buildStartMil == null) {
            this.buildStart = null;
            return;
        }
        Date date = new Date(buildStartMil);
        this.buildStart = date;
    }

    public void currentBuildStart() {
        this.buildStart = DateUtil.getCurrentDate();
    }

    public Date getBuildEnd() {
        return buildEnd;
    }
    
    public Long getBuildEndMil() {
        if (buildEnd == null) return null;
        return buildEnd.getTime();
    }

    public void setBuildEnd(Long buildEndMil) {
        if (buildEndMil == null) {
            this.buildEnd = null;
            return;
        }
        Date date = new Date(buildEndMil);
        this.buildEnd = date;
    }

    public void setBuildEnd(Date buildEnd) {
        this.buildEnd = buildEnd;
    }

    public void currentBuildEnd() {
        this.buildEnd = DateUtil.getCurrentDate();
    }

    /**
     * Dump
     * @param header
     * @return 
     */
    public String dump(String header)
    {
        Properties prop = getProperties();
        return PropertiesUtil.dumpProperties(header, prop);
    }
    
    /**
     * Return the Json value of this TplemStatus
     * @return JSON for TokenStatus
     */
    public String getJson()
    {
        //JsonObject tokenJson = new JsonObject();
        try {
            JSONObject tokenJson = new JSONObject();
            if (nodeIOName != null) {
                tokenJson.put("nodeIOName", nodeIOName);
            }
            if (extractNode != null) {
                tokenJson.put("extractNode", extractNode);
            }
            if (deliveryNode != null) {
                tokenJson.put("deliveryNode", deliveryNode);
            }
            if (objectID != null) {
                tokenJson.put("objectIDS", getObjectIDS());
            }
            if (versionID != null) {
                tokenJson.put("versionID", versionID);
            }
            if (token != null) {
                tokenJson.put("token", token);
            }
            if (anticipatedAvailableDate != null) {
                tokenJson.put("approximateCompletionSeconds", getApproximateCompletionSeconds());
            }
            if (cloudContentBytes != null) {
                tokenJson.put("cloudContentBytes", cloudContentBytes);
            }
            if (archiveContent != null) {
                tokenJson.put("archiveContent", archiveContent.toString());
            }
            if (archiveType != null) {
                tokenJson.put("archiveType", archiveType.toString());
            }
            if (tokenStatusEnum != null) {
                tokenJson.put("tokenStatusEnum", tokenStatusEnum.toString());
            }
            if (fullObject != null) {
                tokenJson.put("fullObject", fullObject);
            }
            if (buildStart != null) {
                tokenJson.put("buildStart", getBuildStartMil());
            }
            if (buildEnd != null) {
                tokenJson.put("buildEnd", getBuildEndMil());
            }
            if (returnOnError != null) {
                tokenJson.put("returnOnError", returnOnError);
            }
            if (exStatus != null) {
                tokenJson.put("exStatus", exStatus);
            }
            if (exMsg != null) {
                tokenJson.put("exMsg", exMsg);
            }
            if ((filterList != null) && (filterList.size() > 0 )) {
                JSONArray jarray = new JSONArray();
                jarray.put(filterList);
                tokenJson.put("filterList", filterList);
            }
            
            return tokenJson.toString();
            
        } catch (Exception ex) {
            System.out.println("TokenStatus Exception:" + ex);
            return null;
        }
    }
    
    /**
     * Get TokenStatus from string Json 
     * @param jsonS representation this class
     * @return TokenStatus
     * @throws TException 
     */
    public static TokenStatus getTokenStatusFromJson(String jsonS)
        throws TException
    {
        try {
            TokenStatus tokenStatus = new TokenStatus();
            //JSONObject obj = new JSONObject(jsonS);
            JSONNull obj = new JSONNull(jsonS);
            tokenStatus.setNodeIOName(obj.getString("nodeIOName"));
            tokenStatus.setExtractNode(obj.getLong("extractNode"));
            tokenStatus.setDeliveryNode(obj.getLong("deliveryNode"));
            tokenStatus.setObjectID(obj.getString("objectIDS"));
            tokenStatus.setVersionID(obj.getInt("versionID"));
            tokenStatus.setToken(obj.getString("token"));
            tokenStatus.setApproximateCompletionDateSeconds(obj.getLong("approximateCompletionSeconds"));
            tokenStatus.setCloudContentBytes(obj.getLong("cloudContentBytes"));
            tokenStatus.setArchiveContent(obj.getString("archiveContent"));
            tokenStatus.setArchiveType(obj.getString("archiveType"));
            tokenStatus.setTokenStatusEnum(obj.getString("tokenStatusEnum"));
            tokenStatus.setFullObject(obj.getBoolean("fullObject"));
            tokenStatus.setReturnOnError(obj.getBoolean("returnOnError"));
            tokenStatus.setExStatus(obj.getInt("exStatus"));
            tokenStatus.setExMsg(obj.getString("exMsg"));
            tokenStatus.setBuildStart(obj.getLong("buildStart"));
            tokenStatus.setBuildEnd(obj.getLong("buildEnd"));
            tokenStatus.setFilterList(obj.getList("filterList"));
            
            return tokenStatus;
        
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    /**
     * Create a Java Properties based on successful completion
     * @return 
     */
    public Properties getProperties()
    {
        Properties prop = new Properties();
        if (nodeIOName != null) {
            prop.setProperty("nodeIOName", nodeIOName);
        }
        if (extractNode != null) {
            prop.setProperty("extractNode", "" + extractNode);
        }
        if (deliveryNode != null) {
            prop.setProperty("deliveryNode", "" + deliveryNode);
        }
        if (objectID != null) {
            prop.setProperty("objectIDS", getObjectIDS());
        }
        if (versionID != null) {
            prop.setProperty("versionID", "" + versionID);
        }
        if (token != null) {
            prop.setProperty("token", token);
        }
        if (anticipatedAvailableDate != null) {
            prop.setProperty("approximateCompletionSeconds", "" + getApproximateCompletionSeconds());
        }
        if (cloudContentBytes != null) {
            prop.setProperty("cloudContentBytes", "" + cloudContentBytes);
        }
        if (archiveContent != null) {
            prop.setProperty("archiveContent", archiveContent.toString());
        }
        if (archiveType != null) {
            prop.setProperty("archiveType", archiveType.toString());
        }
        if (tokenStatusEnum != null) {
            prop.setProperty("tokenStatusEnum", tokenStatusEnum.toString());
        }
        if (fullObject != null) {
            prop.setProperty("fullObject", "" + fullObject);
        }
        if (buildStart != null) {
            prop.setProperty("buildStart", "" + getBuildStartMil());
        }
        if (buildEnd != null) {
            prop.setProperty("buildEnd", "" + getBuildEndMil());
        }
        if (returnOnError != null) {
            prop.setProperty("returnOnError", "" + returnOnError);
        }
        if (exStatus != null) {
            prop.setProperty("exStatus", "" + exStatus);
        }
        if (exMsg != null) {
            prop.setProperty("exMsg", exMsg);
        }
        if ((filterList != null) && (filterList.size() > 0)) {
            System.out.println("filterList.size:" + filterList.size());
            for (int j=0; j < filterList.size(); j++) {
                prop.setProperty("filterList." + (j+1), filterList.get(j)) ;
            }
        }
        return prop;
    }
}