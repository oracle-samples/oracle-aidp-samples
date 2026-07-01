"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * This constructs a default implementation of the {@link OkeWorkloadIdentityAuthenticationDetailsProvider}, constructed
 * in accordance with the following environment variable settings:
 * <ul>
 *
 * <li>{@code KUBERNETES_SERVICE_HOST}:
 * <p>This environment variable represents the Kubernetes service host.</p>
 * </li>
 *
 * <li>{@code KUBERNETES_SERVICE_PORT_PROXYMUX}:
 * <p>This environment variable represents the Kubernetes service port for proxymux.</p>
 * </li>
 *
 * </ul>
 */
const load_from_file_1 = require("./helpers/load-from-file");
const abstract_requesting_authentication_detail_provider_1 = __importDefault(require("./abstract-requesting-authentication-detail-provider"));
const X509_federation_client_for_oke_workload_identity_1 = __importDefault(require("./X509-federation-client-for-oke-workload-identity"));
const session_key_supplier_1 = __importDefault(require("./session-key-supplier"));
const OKE_WORKLOAD_IDENTITY_DEBUG_INFORMATION_LOG = "OKE workload identity can only be used in Enhanced OKE clusters. See https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contenggrantingworkloadaccesstoresources.htm for more info.";
class OkeWorkloadIdentityAuthenticationDetailsProvider extends abstract_requesting_authentication_detail_provider_1.default {
    constructor(federationClient, sessionKeySupplier) {
        super(federationClient, sessionKeySupplier);
        this.federationClient = federationClient;
        this.sessionKeySupplier = sessionKeySupplier;
    }
    // Builder method to create OkeWorkloadIdentityAuthenticationDetailsProviderBuilder which will build
    // OkeWorkloadIdentityAuthenticationDetailsProvider
    static builder(customKubernetesServiceAccountCertPath, customKubernetesServiceAccountTokenPath) {
        return new OkeWorkloadIdentityAuthenticationDetailsProvider.OkeWorkloadIdentityAuthenticationDetailsProviderBuilder(customKubernetesServiceAccountCertPath, customKubernetesServiceAccountTokenPath).build();
    }
    /**
     * Session tokens carry JWT-like claims. Permit the retrieval of the value of those
     * claims from the token.
     * At the least, the token should carry claims for {@link ClaimKeys#COMPARTMENT_ID_CLAIM_KEY} and {@link ClaimKeys#TENANT_ID_CLAIM_KEY}
     * @param key the name of a claim in the session token
     * @return the claim value.
     */
    getStringClaim(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.federationClient.getStringClaim(key);
        });
    }
    /**
     * Refreshes the authentication data used by the provider
     * @return the refreshed authentication data
     */
    refresh() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.federationClient.refreshAndGetSecurityToken();
        });
    }
}
exports.default = OkeWorkloadIdentityAuthenticationDetailsProvider;
OkeWorkloadIdentityAuthenticationDetailsProvider.KUBERNETES_SERVICE_HOST_ENV_VAR_NAME = "KUBERNETES_SERVICE_HOST";
OkeWorkloadIdentityAuthenticationDetailsProvider.KUBERNETES_SERVICE_PORT_PROXYMUX_ENV_VAR_NAME = "KUBERNETES_SERVICE_PORT_PROXYMUX";
OkeWorkloadIdentityAuthenticationDetailsProvider.DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
OkeWorkloadIdentityAuthenticationDetailsProvider.DEFAULT_DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token";
OkeWorkloadIdentityAuthenticationDetailsProvider.ClaimKeys = (_a = class ClaimsKey {
    },
    /**
     * COMPARTMENT_ID is the claim name that the RPST holds for the resource compartment.
     * This can be passed to {@link #getStringClaim} to retrieve the resource's compartment OCID.
     */
    _a.COMPARTMENT_ID_CLAIM_KEY = "res_compartment",
    /**
     * TENANT_ID_CLAIM_KEY is the claim name that the RPST holds for the resource tenancy.
     * This can be passed to {@link #getStringClaim} to retrieve the resource's tenancy OCID.
     */
    _a.TENANT_ID_CLAIM_KEY = "res_tenant",
    _a);
/**
 * Builder for OkeWorkloadIdentityAuthenticationDetailsProvider
 */
OkeWorkloadIdentityAuthenticationDetailsProvider.OkeWorkloadIdentityAuthenticationDetailsProviderBuilder = class OkeWorkloadIdentityAuthenticationDetailsProviderBuilder {
    constructor(customKubernetesServiceAccountCertPath, customKubernetesServiceAccountTokenPath) {
        this.kubernetesServiceAccountCertPath =
            customKubernetesServiceAccountCertPath ||
                OkeWorkloadIdentityAuthenticationDetailsProvider.DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH;
        this.kubernetesServiceAccountTokenPath =
            customKubernetesServiceAccountTokenPath ||
                OkeWorkloadIdentityAuthenticationDetailsProvider.DEFAULT_DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH;
    }
    build() {
        let federationClient;
        let sessionKeySupplier;
        const kubernetesServiceHost = process.env[OkeWorkloadIdentityAuthenticationDetailsProvider.KUBERNETES_SERVICE_HOST_ENV_VAR_NAME];
        if (!kubernetesServiceHost) {
            throw Error(`${OkeWorkloadIdentityAuthenticationDetailsProvider.KUBERNETES_SERVICE_HOST_ENV_VAR_NAME} environment variable is missing. ` +
                OKE_WORKLOAD_IDENTITY_DEBUG_INFORMATION_LOG);
        }
        const kubernetesServiceProxymuxPort = process.env[OkeWorkloadIdentityAuthenticationDetailsProvider
            .KUBERNETES_SERVICE_PORT_PROXYMUX_ENV_VAR_NAME];
        if (!kubernetesServiceProxymuxPort) {
            throw Error(`${OkeWorkloadIdentityAuthenticationDetailsProvider.KUBERNETES_SERVICE_PORT_PROXYMUX_ENV_VAR_NAME} environment variable is missing. ` +
                OKE_WORKLOAD_IDENTITY_DEBUG_INFORMATION_LOG);
        }
        let kubernetesServiceAccountCert;
        try {
            kubernetesServiceAccountCert = load_from_file_1.loadFromFile(this.kubernetesServiceAccountCertPath);
        }
        catch (e) {
            throw Error(`Failed to read ${this.kubernetesServiceAccountCertPath}. ` +
                OKE_WORKLOAD_IDENTITY_DEBUG_INFORMATION_LOG);
        }
        let kubernetesServiceAccountToken;
        try {
            kubernetesServiceAccountToken = load_from_file_1.loadFromFile(this.kubernetesServiceAccountTokenPath);
        }
        catch (e) {
            throw Error(`Failed to read ${this.kubernetesServiceAccountTokenPath}. ` +
                OKE_WORKLOAD_IDENTITY_DEBUG_INFORMATION_LOG);
        }
        // Initialize everything
        sessionKeySupplier = new session_key_supplier_1.default();
        federationClient = new X509_federation_client_for_oke_workload_identity_1.default(`https://${kubernetesServiceHost}:${kubernetesServiceProxymuxPort}/resourcePrincipalSessionTokens`, kubernetesServiceAccountToken, kubernetesServiceAccountCert, sessionKeySupplier);
        return new OkeWorkloadIdentityAuthenticationDetailsProvider(federationClient, sessionKeySupplier);
    }
};
//# sourceMappingURL=oke-workload-identity-authentication-details-provider.js.map