/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import AbstractAuthenticationDetailsProvider from "./models/abstract-authentication-details-provider";
import X509CertificateSupplier from "./models/X509-certificate-supplier";
import { Region } from "../region";
import SessionKeySupplier from "./models/session-key-supplier";
import FederationClient from "./models/federation-client";
import { ResourceDetails } from "./url-based-x509-certificate-supplier";
import CircuitBreaker from "../circuit-breaker";
export default abstract class AbstractFederationClientAuthenticationDetailsProviderBuilder<B extends AbstractFederationClientAuthenticationDetailsProviderBuilder<B, P>, P extends AbstractAuthenticationDetailsProvider> {
    protected template: string;
    protected METADATA_SERVICE_BASE_URL: string;
    private METADATA_AUTH_HEADERS;
    private AUTHORIZATION;
    protected _metadataBaseUrl: string;
    protected _federationEndpoint: string;
    protected _leafCertificateSupplier: X509CertificateSupplier;
    protected _tenancyId: string;
    private _purpose;
    protected region: Region;
    protected _sessionKeySupplier: SessionKeySupplier;
    protected _federationClient: FederationClient;
    protected _intermediateCertificateSuppliers: X509CertificateSupplier[];
    protected _circuitBreaker: any;
    protected _authCircuitBreaker: CircuitBreaker;
    get metadataBaseUrl(): string;
    get federationEndpoint(): string;
    get leafCertificateSupplier(): X509CertificateSupplier;
    get tenancyId(): string;
    get circuitBreaker(): CircuitBreaker;
    get authCircuitBreaker(): CircuitBreaker;
    getRegion(): Region;
    set metadataBaseUrl(metadataBaseUrl: string);
    set federationEndpoint(federationEndpoint: string);
    set leafCertificateSupplier(leafCertificateSupplier: X509CertificateSupplier);
    set tenancyId(tenancyId: string);
    set purpose(purpose: string);
    set sessionKeySupplier(sessionKeySupplier: SessionKeySupplier);
    set intermediateCertificateSuppliers(intermediateCertificateSuppliers: [X509CertificateSupplier]);
    /**
     * Build a new AuthenticationDetailsProvider that uses the FederationClient.
     *
     * @return A new provider instance.
     */
    build(): Promise<P>;
    /**
     * Build the actual provider.
     * @param sessionKeySupplierToUse
     * @return authentication details provider
     */
    protected abstract buildProvider(sessionKeySupplierToUse: SessionKeySupplier): P;
    /**
     * Build the Federation Client
     * @param sessionKeySupplier
     * @return Federation Client
     */
    protected createFederationClient(sessionKeySupplier: SessionKeySupplier): FederationClient;
    /**
     * Auto-detect endpoint and certificate information using Instance metadata.
     */
    protected autoDetectUsingMetadataUrl(): Promise<void>;
    /**
     * Auto detects the endpoint that should be used when talking to OCI Auth, if no endpoint
     * has been configured already.
     * @return The auto-detected, or currently set, auth endpoint.
     */
    protected autoDetectEndpointUsingMetadataUrl(): Promise<string>;
    /**
     * Auto detects and configures the certificates needed using Instance metadata.
     *
     */
    protected autoDetectCertificatesUsingMetadataUrl(): Promise<void>;
    getMetaDataResourceDetail(path: string, circuitBreaker: CircuitBreaker): ResourceDetails;
}
