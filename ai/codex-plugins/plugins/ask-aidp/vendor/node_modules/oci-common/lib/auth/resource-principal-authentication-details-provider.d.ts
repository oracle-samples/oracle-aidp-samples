/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import { Region } from "../region";
import { RegionProvider } from "./auth";
import FederationClient from "./models/federation-client";
import RefreshableOnNotAuthenticatedProvider from "./models/refreshable-on-not-authenticaticated-provider";
import AbstractRequestingAuthenticationDetailsProvider from "./abstract-requesting-authentication-detail-provider";
import SessionKeySupplier from "./models/session-key-supplier";
export default class ResourcePrincipalAuthenticationDetailsProvider extends AbstractRequestingAuthenticationDetailsProvider implements RegionProvider, RefreshableOnNotAuthenticatedProvider<String> {
    protected federationClient: FederationClient;
    protected sessionKeySupplier: SessionKeySupplier;
    protected region: Region;
    static OCI_RESOURCE_PRINCIPAL_VERSION: string;
    static RP_VERSION_2_2: string;
    static OCI_RESOURCE_PRINCIPAL_RPST: string;
    static OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM: string;
    static OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE: string;
    static OCI_RESOURCE_PRINCIPAL_REGION_ENV_VAR_NAME: string;
    constructor(federationClient: FederationClient, sessionKeySupplier: SessionKeySupplier, region: Region);
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
    static builder(): ResourcePrincipalAuthenticationDetailsProvider;
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
    getRegion(): Region;
    /**
     * Builder for ResourcePrincipalAuthenticationDetailsProvider that understands the V2.2 configuration
     */
    static ResourcePrincipalAuthenticationDetailsProviderBuilder: {
        new (): {
            build(): ResourcePrincipalAuthenticationDetailsProvider;
        };
        /**
         * Helper method that interprets the runtime environment to build a v2.2-configured client
         * @return ResourcePrincipalAuthenticationDetailsProvider
         */
        build_2_2(): ResourcePrincipalAuthenticationDetailsProvider;
    };
}
