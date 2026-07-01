// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The cluster configuration to create a new cluster.
*/
export interface NewClusterConfiguration {
    /**
    * Number of worker nodes configured for this cluster. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'numWorkers'?: number;
    'autoScale'?: model.AutoScale;
    /**
    * A unique name for the job cluster.
    */
    'clusterName'?: string;
    /**
    * The Spark version used to run the application.
    */
    'sparkVersion'?: string;
    /**
    * The spark configuration in key-value pairs.
    */
    'sparkConf'?: string;

}

export namespace NewClusterConfiguration {






    export function getJsonObj(obj: NewClusterConfiguration): object {
        const jsonObj = {...obj, ...{
            

                'autoScale': obj.autoScale ?
                
                
                model.AutoScale.getJsonObj(obj.autoScale) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: NewClusterConfiguration): object {
        const jsonObj = {...obj, ...{
            

                    'autoScale': obj.autoScale ?
                
                
                model.AutoScale.getDeserializedJsonObj(obj.autoScale) : undefined,



         }};

        
        
        return jsonObj;
    }
}
