// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to repair a job run.
*/
export interface RepairJobRunDetails {
    /**
    * The collection of selected task IDs to be repaired.
    */
    'taskKeys': Array<string>;
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;

}

export namespace RepairJobRunDetails {



    export function getJsonObj(obj: RepairJobRunDetails): object {
        const jsonObj = {...obj, ...{
            

                'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RepairJobRunDetails): object {
        const jsonObj = {...obj, ...{
            

                    'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
