// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the A2A publish request payload.
*/
export interface AgentCardConfigDetail {
    /**
    * Human-readable agent name.
    */
    'name': string;
    /**
    * Human-readable agent description.
    */
    'description'?: string;
    /**
    * List of skills supported by the agent.
    */
    'skills'?: Array<model.AgentCardSkillDetail>;
    'capabilities'?: model.AgentCardCapabilitiesDetail;
    /**
    * Agent version string.
    */
    'version'?: string;
    'provider'?: model.AgentProvider;
    /**
    * Documentation URL for the agent. Serialized as {@code documentation_url}.
    */
    'documentationUrl'?: string;

}

export namespace AgentCardConfigDetail {








    export function getJsonObj(obj: AgentCardConfigDetail): object {
        const jsonObj = {...obj, ...{
            


                'skills': obj.skills ?
                
                obj.skills.map((item)=>{return model.AgentCardSkillDetail.getJsonObj(item)})
                
                 : undefined,
                'capabilities': obj.capabilities ?
                
                
                model.AgentCardCapabilitiesDetail.getJsonObj(obj.capabilities) : undefined,

                'provider': obj.provider ?
                
                
                model.AgentProvider.getJsonObj(obj.provider) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentCardConfigDetail): object {
        const jsonObj = {...obj, ...{
            


                    'skills': obj.skills ?
                
                obj.skills.map((item)=>{return model.AgentCardSkillDetail.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'capabilities': obj.capabilities ?
                
                
                model.AgentCardCapabilitiesDetail.getDeserializedJsonObj(obj.capabilities) : undefined,

                    'provider': obj.provider ?
                
                
                model.AgentProvider.getDeserializedJsonObj(obj.provider) : undefined,

         }};

        
        
        return jsonObj;
    }
}
