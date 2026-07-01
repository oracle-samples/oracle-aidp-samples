// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to copy the agent flow to target workspace.
*/
export interface CopyAgentFlowDetails {
    /**
    * AgentFlow name.
    */
    'targetDisplayName'?: string;
    /**
    * AgentFlow description.
    */
    'targetDescription'?: string;
    /**
    * Path inside volume where the agentFlow json is written
    */
    'targetPathInfo': string;
    /**
    * Key of the target workspace where the agent flow will be copied.
    */
    'targetWorkspaceKey': string;

}

export namespace CopyAgentFlowDetails {





    export function getJsonObj(obj: CopyAgentFlowDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CopyAgentFlowDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
