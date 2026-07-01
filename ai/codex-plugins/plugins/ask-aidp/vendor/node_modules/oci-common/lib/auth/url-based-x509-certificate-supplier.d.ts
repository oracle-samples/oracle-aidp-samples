/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import { Certificate, PrivateKey } from "sshpk";
import X509CertificateSupplier from "./models/X509-certificate-supplier";
import CertificateAndPrivateKeyPair from "./certificate-and-privatekey-pair";
import Refreshable from "./models/refreshable";
import CircuitBreaker from "../circuit-breaker";
import { Response } from "node-fetch";
/**
 * A class that retrieves certificate based on metadata service url
 */
export declare class URLBasedX509CertificateSupplier implements X509CertificateSupplier, Refreshable {
    private certificateDetails;
    private privateKeyDetails;
    private privateKeyPassphraseCharacters;
    private certificateAndKeyPair;
    constructor(certificateDetails: ResourceDetails, privateKeyDetails: ResourceDetails | null, privateKeyPassphraseCharacters: string | null);
    /**
     * So far we don't care whether the certificate is current or not.
     * @return false always.
     */
    isCurrent(): boolean;
    /**
     * A method to refresh the X509 certificate and return the certificate
     * @returns Promise<URLBasedX509CertificateSupplier>
     */
    refresh(): Promise<URLBasedX509CertificateSupplier>;
    readRawCertificate(certificateDetails: ResourceDetails): Promise<Certificate>;
    readPrivateKey(privateKeyResourceDetails: ResourceDetails | null, privateKeyPassphrase: string | null): Promise<PrivateKey | null>;
    getCertificateAndKeyPair(): CertificateAndPrivateKeyPair;
}
export declare class ResourceDetails {
    private url;
    private headers;
    private circuitBreaker;
    constructor(url: string, headers: Headers, circuitBreaker: CircuitBreaker);
    private METADATA_AUTH_HEADERS;
    private AUTHORIZATION;
    send(): Promise<Response>;
    getUrl(): string;
}
