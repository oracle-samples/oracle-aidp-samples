// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Data Lake Model details
*/
export interface Model {
    /**
    * A unique Id for the model, that is immutable on creation.
    */
    'id': string;
    /**
    * modelName that is immutable on creation.
    */
    'modelName': string;
    /**
    * The model name. It can be changed.
    */
    'displayName'?: string;
    /**
    * The model's schema.
    */
    'namespaceName'?: string;
    /**
    * The description of the Model.
    */
    'description'?: string;
    /**
    * The time the Model was created. An RFC3339 formatted datetime string.
    */
    'timeCreated'?: Date;
    /**
    * The time the Model was updated. An RFC3339 formatted datetime string.
    */
    'timeUpdated'?: Date;
    /**
    * The vendor who created the Model.
    */
    'createdBy'?: string;
    /**
    * The vendor who updated the Model.
    */
    'updatedBy'?: string;
    /**
    * The state of the Model.
    */
    'lifecycleState'?: Model.LifecycleState;
    /**
    * A message describing the current state in more detail. For example, it can be used to provide actionable information for a resource in Failed state.
    */
    'lifecycleDetails'?: string;

   "modelType": string;
}

export namespace Model {










    export enum LifecycleState {
    
    Active = "ACTIVE",
    Creating = "CREATING",
    Deleting = "DELETING",
    Deleted = "DELETED",
    Failed = "FAILED"

}



    export function getJsonObj(obj: Model): object {
        const jsonObj = {...obj, ...{
            











        }};

        
        
        if (obj && "modelType" in obj && obj.modelType) {
            switch (obj.modelType) {
                case "GEN_AI":
                    return model.AiModel.getJsonObj(<model.AiModel>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.modelType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Model): object {
        const jsonObj = {...obj, ...{
            











         }};

        
        
        if (obj && "modelType" in obj && obj.modelType) {
            switch (obj.modelType) {
                case "GEN_AI":
                    return model.AiModel.getDeserializedJsonObj(<model.AiModel>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.modelType}`)
        }
        }
        return jsonObj;
    }
}
