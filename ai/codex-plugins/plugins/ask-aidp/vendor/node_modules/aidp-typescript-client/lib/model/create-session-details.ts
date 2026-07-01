// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a session.
*/
export interface CreateSessionDetails {
    /**
    * UUID of the session.
    */
    'id'?: string;
    /**
    * Path to the session. A directory where notebook server is started and where notebooks are saved. For example, /data/test.ipynb.
    */
    'path'?: string;
    /**
    * A user-friendly name for the session.
    */
    'name'?: string;
    /**
    * Type of session.
    */
    'type'?: string;
    /**
    * Cluster ID.
    */
    'clusterId'?: string;
    /**
    * Key of the agent flow.
    */
    'agentFlowKey'?: string;
    'kernel'?: model.Kernel;

}

export namespace CreateSessionDetails {








    export function getJsonObj(obj: CreateSessionDetails): object {
        const jsonObj = {...obj, ...{
            




                'cluster_id': obj.clusterId,


                'kernel': obj.kernel ?
                
                
                model.Kernel.getJsonObj(obj.kernel) : undefined,
        }};

        delete (jsonObj as Partial<CreateSessionDetails>).clusterId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateSessionDetails): object {
        const jsonObj = {...obj, ...{
            




                'clusterId': (obj as any)["cluster_id"],


                    'kernel': obj.kernel ?
                
                
                model.Kernel.getDeserializedJsonObj(obj.kernel) : undefined,
         }};

        delete (jsonObj as any)["cluster_id"];
        
        return jsonObj;
    }
}
