"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.developerToolConfiguration = exports.OkeWorkloadIdentityAuthenticationDetailsProvider = exports.utils = exports.getChunk = exports.CircuitBreaker = exports.Constants = exports.OciSdkDefaultRetryConfiguration = exports.NoRetryConfigurationDetails = exports.MaxAttemptsTerminationStrategy = exports.FixedTimeDelayStrategy = exports.GenericRetrier = exports.LOG = exports.AbstractRequestingAuthenticationDetailsProvider = exports.ResourcePrincipalAuthenticationDetailsProvider = exports.InstancePrincipalsAuthenticationDetailsProviderBuilder = exports.ConfigFileReader = exports.SessionAuthDetailProvider = exports.ConfigFileAuthenticationDetailsProvider = exports.composeResponse = exports.composeRequest = exports.genericPaginateResponses = exports.paginateResponses = exports.genericPaginateRecords = exports.paginatedRecordsWithLimit = exports.paginatedResponsesWithLimit = exports.paginateRecords = exports.genericTerminalConditionWaiter = exports.genericWaiter = exports.MaxTimeTerminationStrategy = exports.ExponentialBackoffDelayStrategy = exports.EndpointBuilder = exports.Realm = exports.Region = exports.Range = exports.byteLength = exports.ObjectSerializer = exports.convertStringToType = exports.handleErrorBody = exports.mapContainer = exports.handleErrorResponse = exports.getStringFromResponseBody = exports.DefaultRequestSigner = exports.OciError = exports.FetchHttpClient = exports.isRegionProvider = exports.SimpleAuthenticationDetailsProvider = void 0;
const auth = __importStar(require("./lib/auth/auth"));
const error = __importStar(require("./lib/error"));
const signer = __importStar(require("./lib/signer"));
const helper = __importStar(require("./lib/helper"));
const http = __importStar(require("./lib/http"));
const serializer = __importStar(require("./lib/object-serializer"));
const range = __importStar(require("./lib/range"));
const utils = __importStar(require("./lib/utils"));
exports.utils = utils;
const region_1 = require("./lib/region");
Object.defineProperty(exports, "Region", { enumerable: true, get: function () { return region_1.Region; } });
const realm_1 = require("./lib/realm");
Object.defineProperty(exports, "Realm", { enumerable: true, get: function () { return realm_1.Realm; } });
const endpoint_builder_1 = require("./lib/endpoint-builder");
Object.defineProperty(exports, "EndpointBuilder", { enumerable: true, get: function () { return endpoint_builder_1.EndpointBuilder; } });
const log_1 = require("./lib/log");
Object.defineProperty(exports, "LOG", { enumerable: true, get: function () { return log_1.LOG; } });
const constants_1 = __importDefault(require("./lib/constants"));
exports.Constants = constants_1.default;
const circuit_breaker_1 = __importDefault(require("./lib/circuit-breaker"));
exports.CircuitBreaker = circuit_breaker_1.default;
const waiter_1 = require("./lib/waiter");
Object.defineProperty(exports, "ExponentialBackoffDelayStrategy", { enumerable: true, get: function () { return waiter_1.ExponentialBackoffDelayStrategy; } });
Object.defineProperty(exports, "MaxTimeTerminationStrategy", { enumerable: true, get: function () { return waiter_1.MaxTimeTerminationStrategy; } });
Object.defineProperty(exports, "genericWaiter", { enumerable: true, get: function () { return waiter_1.genericWaiter; } });
Object.defineProperty(exports, "genericTerminalConditionWaiter", { enumerable: true, get: function () { return waiter_1.genericTerminalConditionWaiter; } });
Object.defineProperty(exports, "MaxAttemptsTerminationStrategy", { enumerable: true, get: function () { return waiter_1.MaxAttemptsTerminationStrategy; } });
Object.defineProperty(exports, "FixedTimeDelayStrategy", { enumerable: true, get: function () { return waiter_1.FixedTimeDelayStrategy; } });
const retrier_1 = require("./lib/retrier");
Object.defineProperty(exports, "GenericRetrier", { enumerable: true, get: function () { return retrier_1.GenericRetrier; } });
Object.defineProperty(exports, "NoRetryConfigurationDetails", { enumerable: true, get: function () { return retrier_1.NoRetryConfigurationDetails; } });
Object.defineProperty(exports, "OciSdkDefaultRetryConfiguration", { enumerable: true, get: function () { return retrier_1.OciSdkDefaultRetryConfiguration; } });
const instance_principals_authentication_detail_provider_1 = __importDefault(require("./lib/auth/instance-principals-authentication-detail-provider"));
exports.InstancePrincipalsAuthenticationDetailsProviderBuilder = instance_principals_authentication_detail_provider_1.default;
const resource_principal_authentication_details_provider_1 = __importDefault(require("./lib/auth/resource-principal-authentication-details-provider"));
exports.ResourcePrincipalAuthenticationDetailsProvider = resource_principal_authentication_details_provider_1.default;
const oke_workload_identity_authentication_details_provider_1 = __importDefault(require("./lib/auth/oke-workload-identity-authentication-details-provider"));
exports.OkeWorkloadIdentityAuthenticationDetailsProvider = oke_workload_identity_authentication_details_provider_1.default;
const abstract_requesting_authentication_detail_provider_1 = __importDefault(require("./lib/auth/abstract-requesting-authentication-detail-provider"));
exports.AbstractRequestingAuthenticationDetailsProvider = abstract_requesting_authentication_detail_provider_1.default;
const paginators_1 = require("./lib/paginators");
Object.defineProperty(exports, "paginateRecords", { enumerable: true, get: function () { return paginators_1.paginateRecords; } });
Object.defineProperty(exports, "genericPaginateRecords", { enumerable: true, get: function () { return paginators_1.genericPaginateRecords; } });
Object.defineProperty(exports, "paginatedRecordsWithLimit", { enumerable: true, get: function () { return paginators_1.paginatedRecordsWithLimit; } });
Object.defineProperty(exports, "paginateResponses", { enumerable: true, get: function () { return paginators_1.paginateResponses; } });
Object.defineProperty(exports, "genericPaginateResponses", { enumerable: true, get: function () { return paginators_1.genericPaginateResponses; } });
Object.defineProperty(exports, "paginatedResponsesWithLimit", { enumerable: true, get: function () { return paginators_1.paginatedResponsesWithLimit; } });
const config_file_auth_1 = require("./lib/auth/config-file-auth");
Object.defineProperty(exports, "ConfigFileAuthenticationDetailsProvider", { enumerable: true, get: function () { return config_file_auth_1.ConfigFileAuthenticationDetailsProvider; } });
const session_auth_details_provider_1 = require("./lib/auth/session-auth-details-provider");
Object.defineProperty(exports, "SessionAuthDetailProvider", { enumerable: true, get: function () { return session_auth_details_provider_1.SessionAuthDetailProvider; } });
const chunker_1 = __importDefault(require("./lib/chunker"));
exports.getChunk = chunker_1.default;
const config_file_reader_1 = require("./lib/config-file-reader");
Object.defineProperty(exports, "ConfigFileReader", { enumerable: true, get: function () { return config_file_reader_1.ConfigFileReader; } });
const request_generator_1 = require("./lib/request-generator");
Object.defineProperty(exports, "composeRequest", { enumerable: true, get: function () { return request_generator_1.composeRequest; } });
const response_generator_1 = require("./lib/response-generator");
Object.defineProperty(exports, "composeResponse", { enumerable: true, get: function () { return response_generator_1.composeResponse; } });
const developerToolConfiguration = __importStar(require("./lib/developertoolconfiguration"));
exports.developerToolConfiguration = developerToolConfiguration;
exports.SimpleAuthenticationDetailsProvider = auth.SimpleAuthenticationDetailsProvider;
exports.isRegionProvider = auth.isRegionProvider;
exports.FetchHttpClient = http.FetchHttpClient;
exports.OciError = error.OciError;
exports.DefaultRequestSigner = signer.DefaultRequestSigner;
exports.getStringFromResponseBody = helper.getStringFromResponseBody;
exports.handleErrorResponse = helper.handleErrorResponse;
exports.mapContainer = helper.mapContainer;
exports.handleErrorBody = helper.handleErrorBody;
exports.convertStringToType = helper.convertStringToType;
exports.ObjectSerializer = serializer.ObjectSerializer;
exports.byteLength = helper.byteLength;
exports.Range = range.Range;
//# sourceMappingURL=index.js.map