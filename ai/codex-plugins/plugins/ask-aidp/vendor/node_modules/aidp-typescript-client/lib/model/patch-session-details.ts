// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a session.
*/
export interface PatchSessionDetails {
    /**
    * UUID of the session.
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
    'kernel'?: model.Kernel;
    /**
    * Cluster ID.
    */
    'clusterId'?: string;
    /**
    * Set as True, if a new execution context is needed.
    */
    'mustRefreshContext'?: boolean;

}

export namespace PatchSessionDetails {








    export function getJsonObj(obj: PatchSessionDetails): object {
        const jsonObj = {...obj, ...{
            




                'kernel': obj.kernel ?
                
                
                model.Kernel.getJsonObj(obj.kernel) : undefined,
                'cluster_id': obj.clusterId,

                'must_refresh_context': obj.mustRefreshContext,

        }};

        delete (jsonObj as Partial<PatchSessionDetails>).clusterId;delete (jsonObj as Partial<PatchSessionDetails>).mustRefreshContext;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PatchSessionDetails): object {
        const jsonObj = {...obj, ...{
            




                    'kernel': obj.kernel ?
                
                
                model.Kernel.getDeserializedJsonObj(obj.kernel) : undefined,
                'clusterId': (obj as any)["cluster_id"],

                'mustRefreshContext': (obj as any)["must_refresh_context"],

         }};

        delete (jsonObj as any)["cluster_id"];delete (jsonObj as any)["must_refresh_context"];
        
        return jsonObj;
    }
}
