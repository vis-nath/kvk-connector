class KavakConnectorError(Exception):
    pass

class ConfigNotFoundError(KavakConnectorError):
    pass

class QueryError(KavakConnectorError):
    pass

class AuthRequiredError(KavakConnectorError):
    pass
