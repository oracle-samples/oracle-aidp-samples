// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Driver configuration.
*/
export interface DriverConfig {
    /**
    * Node type of optional driver node that encodes the driver node shape and associated resources.
    */
    'driverNodeType'?: string;
    /**
    * Shape of compute cluster driver instance. Example - VM.Standard2.x, VM.Standard.E3.Flex
    */
    'driverShape'?: string;
    'driverShapeConfig'?: model.ShapeConfig;

}

export namespace DriverConfig {




    export function getJsonObj(obj: DriverConfig): object {
        const jsonObj = {...obj, ...{
            


                'driverShapeConfig': obj.driverShapeConfig ?
                
                
                model.ShapeConfig.getJsonObj(obj.driverShapeConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DriverConfig): object {
        const jsonObj = {...obj, ...{
            


                    'driverShapeConfig': obj.driverShapeConfig ?
                
                
                model.ShapeConfig.getDeserializedJsonObj(obj.driverShapeConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
