/*
 * To change this template; choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store.test;

/**
 *
 * @author dloy
 */
public class AddVersionQuery {
    protected Integer nodeID = null;
    protected String objectID = null;
    protected String context = null;
    protected String localID = null;
    protected String manifestRequest = null;
    protected String url = null;
    protected Long size = null;
    protected String type = null;
    protected String value = null;
    protected String formatType = null;

    public String dump(String header)
    {
        return header
                + " - nodeID=" + nodeID
                + " - objectID=" + objectID
                + " - context=" + context
                + " - localID=" + localID
                + " - manifestRequest=" + manifestRequest
                + " - url=" + url
                + " - size=" + size
                + " - type=" + type
                + " - value=" + value
                + " - formatType=" + formatType
                ;
    }
    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getFormatType() {
        return formatType;
    }

    public void setFormatType(String formatType) {
        this.formatType = formatType;
    }

    public String getLocalID() {
        return localID;
    }

    public void setLocalID(String localID) {
        this.localID = localID;
    }

    public String getManifestRequest() {
        return manifestRequest;
    }

    public void setManifestRequest(String manifestRequest) {
        this.manifestRequest = manifestRequest;
    }

    public Integer getNodeID() {
        return nodeID;
    }

    public void setNodeID(Integer nodeID) {
        this.nodeID = nodeID;
    }

    public String getObjectID() {
        return objectID;
    }

    public void setObjectID(String objectID) {
        this.objectID = objectID;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


}
