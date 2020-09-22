/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.store;

import org.cdlib.mrt.utility.StringUtil;

/**
 * Specifies the "Media-type", and "Access-type"
 * properties.  The media types are "magnetic-disk", "magnetic-tape",
 * "optical-disk", and "solid-state"; the access types are "on-line",
 * "near-line" and "off-line".
 * @author dloy
 */
public enum StorageTypesEnum {

    content_addressable("media", "content-addressable"),
    magnetic_disk("media", "magnetic-disk"),
    magnetic_tape("media", "magnetic-tape"),
    optical_disk("media", "optical-disk"),
    solid_state("media", "solid-state"),
    media_unknown("media", "unknown"),
    on_line("access", "on-line"),
    near_line("access", "near-line"),
    off_line("access", "off-line"),
    access_unknown("access", "unknown"),
    virtual("protocol", "virtual"),
    zfs("protocol", "zfs"),
    s3("protocol", "s3"),
    protocol_unknown("protocol", "unknown"),
    physical_node("nodeform", "physical"),
    virtual_node("nodeform", "virtual"),
    unknown_node("nodeform", "unknown"),
    file_procotol("nodeprotocol", "file"),
    http_protocol("nodeprotocol", "http"),
    cloud("connectivity", "cloud"),
    san("connectivity", "san"),
    connectivity_unknown("connectivity", "unknown"),
    aws("provider", "aws"),
    nodeio("provider", "nodeio"),
    sdsc("provider", "sdsc");

    protected String type = null;
    protected String name = null;

    StorageTypesEnum(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    /**
     * Return the storage type
     * @return storage type
     */
    public String getType()
    {
        return this.type;
    }

    /**
     * return the description of the storage
     * @return storage description
     */
    public String getName()
    {
        return this.name;
    }


    /**
     * Match the Storage type to type and description
     * @param type storage type
     * @param name storage description
     * @return enumerated StorageType value
     */
    public static StorageTypesEnum valueOf(String type, String name)
    {
        boolean DEBUG = false;
            if (DEBUG) System.out.println("StorageTypesEnum"
                    + " - type=" + type
                    + " - name=" + name
                    );
        if (StringUtil.isEmpty(type) || StringUtil.isEmpty(name)) return null;
        type = type.toLowerCase();
        name = name.toLowerCase();
        for (StorageTypesEnum p : StorageTypesEnum.values()) {
            if (DEBUG) System.out.println("StorageTypesEnum"
                    + " - p.getType()=" + p.getType()
                    + " - p.getName()=" + p.getName()
                    );
            if (p.getType().equals(type) && p.getName().equals(name)) {
                return p;
            }
        }
        return null;
    }
}
