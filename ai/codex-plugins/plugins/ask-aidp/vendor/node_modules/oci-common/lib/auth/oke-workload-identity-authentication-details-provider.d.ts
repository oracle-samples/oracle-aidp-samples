/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import FederationClient from "./models/federation-client";
import RefreshableOnNotAuthenticatedProvider from "./models/refreshable-on-not-authenticaticated-provider";
import AbstractRequestingAuthenticationDetailsProvider from "./abstract-requesting-authentication-detail-provider";
import SessionKeySupplier from "./models/session-key-supplier";
export default class OkeWorkloadIdentityAuthenticationDetailsProvider extends AbstractRequestingAuthenticationDetailsProvider implements RefreshableOnNotAuthenticatedProvider<String> {
    protected federationClient: FederationClient;
    protected sessionKeySupplier: SessionKeySupplier;
    static KUBERNETES_SERVICE_HOST_ENV_VAR_NAME: string;
    static KUBERNETES_SERVICE_PORT_PROXYMUX_ENV_VAR_NAME: string;
    static DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH: string;
    static DEFAULT_DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH: string;
    protected _sessionKeySupplier: SessionKeySupplier;
    protected _federationClient: FederationClient;
    constructor(federationClient: FederationClient, sessionKeySupplier: SessionKeySupplier);
    static ClaimKeys: {
        new (): {};
        /**
         * COMPARTMENT_ID is the claim name that the RPST holds for the resource compartment.
         * This can be passed to {@link #getStringClaim} to retrieve the resource's compartment OCID.
         */
        COMPARTMENT_ID_CLAIM_KEY: string;
        /**
         * TENANT_ID_CLAIM_KEY is the claim name that the RPST holds for the resource tenancy.
         * This can be passed to {@link #getStringClaim} to retrieve the resource's tenancy OCID.
         */
        TENANT_ID_CLAIM_KEY: string;
    };
    static builder(customKubernetesServiceAccountCertPath: string, customKubernetesServiceAccountTokenPath: string): OkeWorkloadIdentityAuthenticationDetailsProvider;
    /**
     * Session tokens carry JWT-like claims. Permit the retrieval of the value of those
     * claims from the token.
     * At the least, the token should carry claims for {@link ClaimKeys#COMPARTMENT_ID_CLAIM_KEY} and {@link ClaimKeys#TENANT_ID_CLAIM_KEY}
     * @param key the name of a claim in the session token
     * @return the claim value.
     */
    getStringClaim(key: string): Promise<string | null>;
    /**
     * Refreshes the authentication data used by the provider
     * @return the refreshed authentication data
     */
    refresh(): Promise<string>;
    /**
     * Builder for OkeWorkloadIdentityAuthenticationDetailsProvider
     */
    static OkeWorkloadIdentityAuthenticationDetailsProviderBuilder: {
        new (customKubernetesServiceAccountCertPath?: string | undefined, customKubernetesServiceAccountTokenPath?: string | undefined): {
            kubernetesServiceAccountCertPath: string;
            kubernetesServiceAccountTokenPath: string;
            build(): OkeWorkloadIdentityAuthenticationDetailsProvider;
        };
    };
}
