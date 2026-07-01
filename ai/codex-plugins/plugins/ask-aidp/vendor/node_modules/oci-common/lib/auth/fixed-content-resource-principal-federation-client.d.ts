/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import SessionKeySupplier from "./models/session-key-supplier";
import FederationClient from "./models/federation-client";
/**
 * This class returns a security token, supplied as a fixed value.
 */
export default class FixedContentResourcePrincipalFederationClient implements FederationClient {
    private securityTokenAdapter;
    /**
     * Constructor of FixedContentResourcePrincipalFederationClient.
     *
     * @param resourcePrincipalSessionToken the constant value for the RPST
     * @param sessionKeySupplier the associated SessionKeySupplier
     */
    constructor(resourcePrincipalSessionToken: string, sessionKeySupplier: SessionKeySupplier);
    getSecurityToken(): Promise<string>;
    refreshAndGetSecurityToken(): Promise<string>;
    getStringClaim(key: string): Promise<string | null>;
}
