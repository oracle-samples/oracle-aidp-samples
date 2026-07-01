// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Data Lake AiModel details
* 
*/
export interface AiModel extends model.Model {
    /**
    * Provides Ai Model's Capabilities.
    */
    'modelCapabilities'?: Array<model.AiModelCapabilitiesEnum>;
    /**
    * version that is available for that AI Model.
    */
    'modelVersion'?: string;
    /**
    * vendor name for that Model.
    */
    'vendor'?: string;
    /**
    * region source of that model
    */
    'regionId'?: string;

   "modelType": string;
}

export namespace AiModel {





    export function getJsonObj(obj: AiModel, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Model.getJsonObj(obj) as AiModel, ...{
            
                'modelCapabilities': obj.modelCapabilities ?
                
                obj.modelCapabilities.map((item)=>{return model.AiModelCapabilitiesEnum.getJsonObj(item)})
                
                 : undefined,



        }};

        
        
        return jsonObj;
    }
    export const modelType = 'GEN_AI';
    export function getDeserializedJsonObj(obj: AiModel, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Model.getDeserializedJsonObj(obj) as AiModel, ...{
            
                    'modelCapabilities': obj.modelCapabilities ?
                
                obj.modelCapabilities.map((item)=>{return model.AiModelCapabilitiesEnum.getDeserializedJsonObj(item)})
                
                 : undefined,



         }};

        
        
        return jsonObj;
    }
}
