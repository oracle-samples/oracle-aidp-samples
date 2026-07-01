// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of each Model Parameter
*/
export interface ModelParameterResponse {
    'maximumOutputTokens'?: model.ModelParameterDetail;
    'temperature'?: model.ModelParameterDetail;
    'topP'?: model.ModelParameterDetail;
    'topK'?: model.ModelParameterDetail;
    'frequencyPenalty'?: model.ModelParameterDetail;
    'presencePenalty'?: model.ModelParameterDetail;
    'seed'?: model.ModelParameterDetail;
    'reasoningEffort'?: model.ModelParameterDetail;
    'numberOfGenerations'?: model.ModelParameterDetail;
    'truncate'?: model.ModelParameterDetail;
    'preambleOverride'?: model.ModelParameterDetail;
    'safetyMode'?: model.ModelParameterDetail;

}

export namespace ModelParameterResponse {













    export function getJsonObj(obj: ModelParameterResponse): object {
        const jsonObj = {...obj, ...{
            
                'maximumOutputTokens': obj.maximumOutputTokens ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.maximumOutputTokens) : undefined,
                'temperature': obj.temperature ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.temperature) : undefined,
                'topP': obj.topP ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.topP) : undefined,
                'topK': obj.topK ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.topK) : undefined,
                'frequencyPenalty': obj.frequencyPenalty ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.frequencyPenalty) : undefined,
                'presencePenalty': obj.presencePenalty ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.presencePenalty) : undefined,
                'seed': obj.seed ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.seed) : undefined,
                'reasoningEffort': obj.reasoningEffort ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.reasoningEffort) : undefined,
                'numberOfGenerations': obj.numberOfGenerations ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.numberOfGenerations) : undefined,
                'truncate': obj.truncate ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.truncate) : undefined,
                'preambleOverride': obj.preambleOverride ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.preambleOverride) : undefined,
                'safetyMode': obj.safetyMode ?
                
                
                model.ModelParameterDetail.getJsonObj(obj.safetyMode) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelParameterResponse): object {
        const jsonObj = {...obj, ...{
            
                    'maximumOutputTokens': obj.maximumOutputTokens ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.maximumOutputTokens) : undefined,
                    'temperature': obj.temperature ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.temperature) : undefined,
                    'topP': obj.topP ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.topP) : undefined,
                    'topK': obj.topK ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.topK) : undefined,
                    'frequencyPenalty': obj.frequencyPenalty ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.frequencyPenalty) : undefined,
                    'presencePenalty': obj.presencePenalty ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.presencePenalty) : undefined,
                    'seed': obj.seed ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.seed) : undefined,
                    'reasoningEffort': obj.reasoningEffort ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.reasoningEffort) : undefined,
                    'numberOfGenerations': obj.numberOfGenerations ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.numberOfGenerations) : undefined,
                    'truncate': obj.truncate ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.truncate) : undefined,
                    'preambleOverride': obj.preambleOverride ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.preambleOverride) : undefined,
                    'safetyMode': obj.safetyMode ?
                
                
                model.ModelParameterDetail.getDeserializedJsonObj(obj.safetyMode) : undefined,
         }};

        
        
        return jsonObj;
    }
}
