// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An output slot for a node, including type and requirements.
*/
export interface NodeOutput {
    /**
    * Unique name for the output slot.
    */
    'name': string;
    'type': model.NodeIo;
    /**
    * UI position of the output port on the node.
    */
    'portPosition'?: model.NodePortPosition;
    /**
    * Documentation or tooltip shown in the UI.
    */
    'description'?: string;
    /**
    * True if this output is mandatory.
    */
    'isRequired': boolean;

}

export namespace NodeOutput {






    export function getJsonObj(obj: NodeOutput): object {
        const jsonObj = {...obj, ...{
            

                'type': obj.type ?
                
                
                model.NodeIo.getJsonObj(obj.type) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: NodeOutput): object {
        const jsonObj = {...obj, ...{
            

                    'type': obj.type ?
                
                
                model.NodeIo.getDeserializedJsonObj(obj.type) : undefined,



         }};

        
        
        return jsonObj;
    }
}
