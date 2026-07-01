// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Transition details of the model version.
*/
export interface TransitionModelVersionStageDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Model\u2019s version number.
    */
    'version': string;
    /**
    * New stage for the model version.
    */
    'stage': string;
    /**
    * Whether to archive existing versions in the target stage.
    */
    'archiveExistingVersions': boolean;

}

export namespace TransitionModelVersionStageDetails {





    export function getJsonObj(obj: TransitionModelVersionStageDetails): object {
        const jsonObj = {...obj, ...{
            



                'archive_existing_versions': obj.archiveExistingVersions,

        }};

        delete (jsonObj as Partial<TransitionModelVersionStageDetails>).archiveExistingVersions;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TransitionModelVersionStageDetails): object {
        const jsonObj = {...obj, ...{
            



                'archiveExistingVersions': (obj as any)["archive_existing_versions"],

         }};

        delete (jsonObj as any)["archive_existing_versions"];
        
        return jsonObj;
    }
}
