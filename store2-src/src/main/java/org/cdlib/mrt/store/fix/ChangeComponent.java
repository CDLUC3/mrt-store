
package org.cdlib.mrt.store.fix;

/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TException;


public class ChangeComponent
{
    
    protected static final String NAME = "ChangeTokenFix";
    protected static final String MESSAGE = NAME + ": ";
    
    //public enum Operation { none, asis, move, reference, delete };
    public enum Operation { none, asis_data, asis_reference, move_data, move_reference, provenance, delete };
    
    protected Long nodeID = null;
    protected Identifier objectID = null;
    protected Integer versionID = null;
    protected FileComponent inComponent = null;
    protected FileComponent outComponent = null;
    protected Operation op = Operation.none;
    
    public static ChangeComponent get(
            Long nodeID, 
            Identifier objectID,
            Integer versionID,
            FileComponent inComponent)
        throws TException
    {
        ChangeComponent component = new ChangeComponent(nodeID, objectID, versionID).setInComponent(inComponent);
        return component;
    }
                    
    public ChangeComponent(
            Long nodeID, 
            Identifier objectID,
            Integer versionID)
        throws TException
    {
        this.nodeID = nodeID;
        this.objectID = objectID;
        this.versionID = versionID;
    }

    public Long getNodeID() {
        return nodeID;
    }

    public void setNodeID(Long nodeID) {
        this.nodeID = nodeID;
    }

    public Identifier getObjectID() {
        return objectID;
    }

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    public Integer getVersionID() {
        return versionID;
    }

    public void setVersionID(Integer versionID) {
        this.versionID = versionID;
    }

    public FileComponent getInComponent() {
        return inComponent;
    }

    public ChangeComponent setInComponent(FileComponent inComponent) {
        this.inComponent = inComponent;
        return this;
    }

    public FileComponent getOutComponent() {
        return outComponent;
    }

    public ChangeComponent setOutComponent(FileComponent outComponent) {
        this.outComponent = outComponent;
        return this;
    }

    public Operation getOp() {
        return op;
    }

    public ChangeComponent setOp(Operation op) {
        this.op = op;
        return this;
    }

    public ChangeComponent setOp(String opS) {
        if (opS != null) {
            this.op = op.valueOf(opS);
        } else {
            this.op = null;
        }
        return this;
    }
    
    public String dump(String header)
    {
        String out = header 
                + " - nodeID:" +  nodeID
                + " - objectID:" +  objectID.getValue()
                + " - versionID:" +  versionID
                + " - IN:" + inComponent.dump("inComponent")
                + " - OUT:" + outComponent.dump("outComponent")
                ;
        return out;
                
    }
}
