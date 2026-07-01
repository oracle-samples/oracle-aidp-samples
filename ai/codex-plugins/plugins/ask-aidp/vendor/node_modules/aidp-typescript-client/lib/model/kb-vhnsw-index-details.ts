// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* HNSW (Hierarchical Navigable Small World) index parameters
*/
export interface KbVHnswIndexDetails {
    /**
    * Distance metric for the vector index
    */
    'distance'?: KbVHnswIndexDetails.Distance;
    /**
    * Target accuracy percentage for the index (1-100) Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'targetAccuracy'?: number;
    /**
    * Maximum number of neighbors each vector can have on any layer (M parameter) Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'neighbors'?: number;
    /**
    * Maximum number of closest vector candidates considered during index construction Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'efConstruction'?: number;

}

export namespace KbVHnswIndexDetails {

    export enum Distance {
    
    Cosine = "COSINE",
    Euclidean = "EUCLIDEAN",
    L2Squared = "L2_SQUARED",
    Dot = "DOT",
    Manhattan = "MANHATTAN",
    Hamming = "HAMMING",
    Jaccard = "JACCARD"

}





    export function getJsonObj(obj: KbVHnswIndexDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KbVHnswIndexDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
