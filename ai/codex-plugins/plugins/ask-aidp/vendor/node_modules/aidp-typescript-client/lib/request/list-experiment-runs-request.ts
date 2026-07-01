// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/ListExperimentRuns.ts.html |here} to see how to use ListExperimentRunsRequest.
 */
export interface ListExperimentRunsRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the Workspace
 */
 'workspaceKey': string;
/**
 * Details of experiment runs to fetch.
 */
 'listExperimentRunsDetails':  model.ListExperimentRunsDetails;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
/**
 * The DH User Principal Header .
 */
 'dhUserPrincipal'?: string;
}

