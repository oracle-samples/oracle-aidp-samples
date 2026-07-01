// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information of tool in the schema.
*/
export interface ToolSummary {
    /**
    * The unique identifier of the tool
    */
    'key': string;
    /**
    * Tool name.
    */
    'displayName': string;
    /**
    * Type of tool. Managed, external or mount tool.
    */
    'toolType': model.ToolType;
    /**
    * The key of the Workspace to which this tool belongs.
    */
    'workspaceKey'?: string;
    /**
    * Tool description.
    */
    'description'?: string;
    /**
    * The date and time the tool was created.
    */
    'timeCreated'?: Date;
    /**
    * The date and time the tool was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The OCID of the user/principal who created the tool.
    */
    'createdBy'?: string;
    /**
    * The ID of the user who last updated the schema.
    */
    'updatedBy'?: string;

}

export namespace ToolSummary {










    export function getJsonObj(obj: ToolSummary): object {
        const jsonObj = {...obj, ...{
            









        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ToolSummary): object {
        const jsonObj = {...obj, ...{
            









         }};

        
        
        return jsonObj;
    }
}
