// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Prompt attacks prevention safety policy
*/
export interface PromptAttacksPreventionPolicy extends model.SafetyPolicy {

   "policyType": string;
}

export namespace PromptAttacksPreventionPolicy {

    export function getJsonObj(obj: PromptAttacksPreventionPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getJsonObj(obj) as PromptAttacksPreventionPolicy, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const policyType = 'PROMPT_ATTACKS_PREVENTION';
    export function getDeserializedJsonObj(obj: PromptAttacksPreventionPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getDeserializedJsonObj(obj) as PromptAttacksPreventionPolicy, ...{
            
         }};

        
        
        return jsonObj;
    }
}
