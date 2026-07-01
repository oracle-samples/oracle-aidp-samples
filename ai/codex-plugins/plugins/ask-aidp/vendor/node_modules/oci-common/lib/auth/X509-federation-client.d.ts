/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import FederationClient from "./models/federation-client";
import SessionKeySupplier from "./models/session-key-supplier";
import X509CertificateSupplier from "./models/X509-certificate-supplier";
import SecurityTokenAdapter from "./security-token-adapter";
import { FetchHttpClient } from "../http";
import CircuitBreaker from "../circuit-breaker";
export default class X509FederationClient implements FederationClient {
    private federationEndpoint;
    private _tenancyId;
    private _leafCertificateSupplier;
    private sessionKeySupplier;
    private intermediateCertificateSuppliers;
    private purpose;
    private circuitBreaker;
    securityTokenAdapter: SecurityTokenAdapter;
    private _circuitBreaker;
    private static DEFAULT_AUTH_MAX_RETRY_COUNT;
    private static DEFAULT_AUTH_MAX_DELAY_IN_SECONDS;
    private static defaultAuthRetryConfiguration;
    httpClient: FetchHttpClient;
    constructor(federationEndpoint: string, _tenancyId: string, _leafCertificateSupplier: X509CertificateSupplier, sessionKeySupplier: SessionKeySupplier, intermediateCertificateSuppliers: X509CertificateSupplier[], purpose: string, circuitBreaker: CircuitBreaker);
    get tenancyId(): string;
    get leafCertificateSupplier(): X509CertificateSupplier;
    close(): void;
    /**
     * Gets a security token. If there is already a valid token cached, it will be returned. Else this will make a call
     * to the auth service to get a new token, using the provided suppliers.
     *
     * This method is thread-safe.
     * @return the security token
     * @throws OciError If there is any issue with getting a token from the auth server
     */
    getSecurityToken(): Promise<string>;
    /**
     * Return a claim embedded in the security token
     * @param key the name of the claim
     * @return the value of the claim or null if unable to find
     */
    getStringClaim(key: string): Promise<string | null>;
    refreshAndGetSecurityToken(): Promise<string>;
    private refreshAndGetSecurityTokenInner;
    /**
     * Gets a security token from the federation server
     * @return the security token, which is basically a JWT token string
     */
    private getSecurityTokenFromServer;
    private getTokenAsync;
}
