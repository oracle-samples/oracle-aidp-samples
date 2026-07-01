// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a notebook session.
*/
export interface SessionSummary {
    /**
    * UUID if the session.
    */
    'id'?: string;
    /**
    * A user-friendly name for the session.
    */
    'name'?: string;
    /**
    * Path to the session. A directory where notebook server is started and where notebooks are saved. For example, /data/test.ipynb.
    */
    'path'?: string;
    /**
    * Type of session.
    */
    'type'?: string;
    /**
    * Cluster ID.
    */
    'clusterId'?: string;
    'kernel'?: model.KernelSummary;
    /**
    * Agent flow key of an agent flow.
    */
    'agentFlowKey'?: string;
    /**
    * lifecycleState of a Notebook Session.
    */
    'lifecycleState'?: string;

}

export namespace SessionSummary {









    export function getJsonObj(obj: SessionSummary): object {
        const jsonObj = {...obj, ...{
            




                'cluster_id': obj.clusterId,

                'kernel': obj.kernel ?
                
                
                model.KernelSummary.getJsonObj(obj.kernel) : undefined,


        }};

        delete (jsonObj as Partial<SessionSummary>).clusterId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SessionSummary): object {
        const jsonObj = {...obj, ...{
            




                'clusterId': (obj as any)["cluster_id"],

                    'kernel': obj.kernel ?
                
                
                model.KernelSummary.getDeserializedJsonObj(obj.kernel) : undefined,


         }};

        delete (jsonObj as any)["cluster_id"];
        
        return jsonObj;
    }
}
