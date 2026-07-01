// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* IVF (Inverted File Flat) index parameters
*/
export interface KbVIvfIndexDetails {
    /**
    * Distance metric for the vector index
    */
    'distance'?: KbVIvfIndexDetails.Distance;
    /**
    * Target accuracy percentage for the index (1-100) Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'targetAccuracy'?: number;
    /**
    * Number of partitions (clusters) to divide the vector data into Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'neighborPartitions'?: number;
    /**
    * Maximum number of partitions to probe during a search. Higher values increase accuracy but may reduce performance Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'neighborPartitionProbes'?: number;

}

export namespace KbVIvfIndexDetails {

    export enum Distance {
    
    Cosine = "COSINE",
    Euclidean = "EUCLIDEAN",
    L2Squared = "L2_SQUARED",
    Dot = "DOT",
    Manhattan = "MANHATTAN",
    Hamming = "HAMMING",
    Jaccard = "JACCARD"

}





    export function getJsonObj(obj: KbVIvfIndexDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KbVIvfIndexDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
