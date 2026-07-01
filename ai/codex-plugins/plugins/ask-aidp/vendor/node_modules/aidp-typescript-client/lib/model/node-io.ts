// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Defines the data type and color for an input or output.
*/
export interface NodeIo {
    /**
    * Possible types of node inputs and outputs.
    */
    'dataType': NodeIo.DataType;
    'color': model.NodeIoColor;

}

export namespace NodeIo {

    export enum DataType {
    
    Flow = "FLOW",
    Str = "STR",
    Int = "INT",
    Bool = "BOOL",
    Float = "FLOAT",
    List = "LIST",
    Dict = "DICT",
    Any = "ANY",
    Tools = "TOOLS",
    Llms = "LLMS",
    Agents = "AGENTS",
    Planner = "PLANNER",
    Flows = "FLOWS"

}



    export function getJsonObj(obj: NodeIo): object {
        const jsonObj = {...obj, ...{
            

                'color': obj.color ?
                
                
                model.NodeIoColor.getJsonObj(obj.color) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: NodeIo): object {
        const jsonObj = {...obj, ...{
            

                    'color': obj.color ?
                
                
                model.NodeIoColor.getDeserializedJsonObj(obj.color) : undefined,
         }};

        
        
        return jsonObj;
    }
}
