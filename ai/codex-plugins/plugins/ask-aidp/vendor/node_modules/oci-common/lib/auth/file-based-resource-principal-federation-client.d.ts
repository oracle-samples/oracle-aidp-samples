import FederationClient from "./models/federation-client";
import SessionKeySupplier from "./models/session-key-supplier";
import SecurityTokenAdapter from "./security-token-adapter";
/**
 * This class gets a security token from file.
 */
export default class FileBasedResourcePrincipalFederationClient implements FederationClient {
    private sessionKeySupplier;
    private resourcePrincipalSessionTokenPath;
    private securityTokenAdapter;
    constructor(sessionKeySupplier: SessionKeySupplier, resourcePrincipalSessionTokenPath: string);
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
     * Gets a security token from file
     * @return the security token, which is basically a JWT token string
     */
    protected getSecurityTokenFromFile(): SecurityTokenAdapter;
}
