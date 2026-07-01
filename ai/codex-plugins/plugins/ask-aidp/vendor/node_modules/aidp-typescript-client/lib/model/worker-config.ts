// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Worker configuration.
*/
export interface WorkerConfig {
    /**
    * Shape of the compute cluster executor instance.
    */
    'workerShape'?: string;
    'workerShapeConfig'?: model.ShapeConfig;
    /**
    * Minimum number of workers. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'minWorkerCount'?: number;
    /**
    * Maximum number of workers. When this property is specified, the cluster is auto-scaled. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxWorkerCount'?: number;

}

export namespace WorkerConfig {





    export function getJsonObj(obj: WorkerConfig): object {
        const jsonObj = {...obj, ...{
            

                'workerShapeConfig': obj.workerShapeConfig ?
                
                
                model.ShapeConfig.getJsonObj(obj.workerShapeConfig) : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkerConfig): object {
        const jsonObj = {...obj, ...{
            

                    'workerShapeConfig': obj.workerShapeConfig ?
                
                
                model.ShapeConfig.getDeserializedJsonObj(obj.workerShapeConfig) : undefined,


         }};

        
        
        return jsonObj;
    }
}
