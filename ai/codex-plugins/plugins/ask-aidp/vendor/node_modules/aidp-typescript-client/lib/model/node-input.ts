// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An input slot for a node, including type and requirements.
*/
export interface NodeInput {
    /**
    * Unique name for the input slot.
    */
    'name': string;
    'type': model.NodeIo;
    /**
    * UI position of the input port on the node.
    */
    'portPosition'?: model.NodePortPosition;
    /**
    * Documentation or tooltip shown in the UI.
    */
    'description'?: string;
    /**
    * True if this input is mandatory.
    */
    'isRequired': boolean;
    /**
    * True if only one connection is allowed; false allows multiple.
    */
    'isSingleConnection'?: boolean;

}

export namespace NodeInput {







    export function getJsonObj(obj: NodeInput): object {
        const jsonObj = {...obj, ...{
            

                'type': obj.type ?
                
                
                model.NodeIo.getJsonObj(obj.type) : undefined,




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: NodeInput): object {
        const jsonObj = {...obj, ...{
            

                    'type': obj.type ?
                
                
                model.NodeIo.getDeserializedJsonObj(obj.type) : undefined,




         }};

        
        
        return jsonObj;
    }
}
