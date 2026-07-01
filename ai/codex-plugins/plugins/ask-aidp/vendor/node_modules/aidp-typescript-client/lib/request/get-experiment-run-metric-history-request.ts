// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/GetExperimentRunMetricHistory.ts.html |here} to see how to use GetExperimentRunMetricHistoryRequest.
 */
export interface GetExperimentRunMetricHistoryRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the Workspace
 */
 'workspaceKey': string;
/**
 * ID of the run metric history to fetch.
 */
 'runId': string;
/**
 * Name of the metric key.
 */
 'metricKey': string;
/**
 * Pagination token to go to the next page of metric history.
 */
 'pageToken'?: string;
/**
 * Maximum number of logged instances of a metric for a run to return per call. Backend servers 
* may restrict the value of max_results depending on performance requirements. Requests that do 
* not specify this value will behave as non-paginated queries where all metric history values 
* for a given metric within a run are returned in a single response.
* 
 */
 'maxResults'?: number;
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

