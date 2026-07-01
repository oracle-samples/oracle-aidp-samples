// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Service provider information for an agent.
*/
export interface AgentProvider {
    /**
    * The name of the agent provider's organization.
    */
    'organization'?: string;
    /**
    * A URL for the agent provider's website or documentation.
    */
    'url'?: string;

}

export namespace AgentProvider {



    export function getJsonObj(obj: AgentProvider): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentProvider): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
