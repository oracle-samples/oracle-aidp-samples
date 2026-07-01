// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Items of AgentFlow Dependencies
*/
export interface AgentFlowDependencyItem {
    /**
    * AICompute/File
    */
    'type'?: string;
    /**
    * AICompute key
    */
    'key'?: string;
    /**
    * Location of file/folders
    */
    'location'?: string;

}

export namespace AgentFlowDependencyItem {




    export function getJsonObj(obj: AgentFlowDependencyItem): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowDependencyItem): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
