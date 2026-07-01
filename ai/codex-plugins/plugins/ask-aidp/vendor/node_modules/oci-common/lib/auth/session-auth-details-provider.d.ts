import { AuthenticationDetailsProvider, RegionProvider } from "./auth";
import { ConfigFileAuthenticationDetailsProvider } from "./config-file-auth";
export declare class SessionAuthDetailProvider extends ConfigFileAuthenticationDetailsProvider implements AuthenticationDetailsProvider, RegionProvider {
    constructor(configurationFilePath?: string, profile?: string);
    getKeyId(): Promise<string>;
    getSecurityToken(): Promise<string>;
    refreshSessionToken(): Promise<string>;
}
