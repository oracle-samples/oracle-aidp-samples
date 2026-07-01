// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single skill in the publish request.
*/
export interface AgentCardSkillDetail {
    /**
    * Unique skill identifier.
    */
    'id': string;
    /**
    * Display name of the skill.
    */
    'name': string;
    /**
    * Description of what the skill does.
    */
    'description'?: string;
    /**
    * Optional tags for categorization/search.
    */
    'tags'?: Array<string>;
    /**
    * Optional example prompts for this skill.
    */
    'examples'?: Array<string>;

}

export namespace AgentCardSkillDetail {






    export function getJsonObj(obj: AgentCardSkillDetail): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentCardSkillDetail): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
