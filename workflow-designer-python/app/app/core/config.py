import secrets
from typing import Any, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, BaseSettings, EmailStr, HttpUrl, PostgresDsn, validator


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    # SECRET_KEY: str = secrets.token_urlsafe(32)
    # # 60 minutes * 24 hours * 8 days = 8 days
    # ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    SERVER_NAME: str
    SERVER_HOST: AnyHttpUrl
    # # WORKFLOW_DESIGNER_PYTHON_CORS_ORIGINS is a JSON-formatted list of origins
    # # e.g: '["http://localhost", "http://localhost:4200", "http://localhost:3000", \
    # # "http://localhost:8080", "http://local.dockertoolbox.tiangolo.com"]'
    WORKFLOW_DESIGNER_PYTHON_CORS_ORIGINS: List[AnyHttpUrl] = []

    @validator("WORKFLOW_DESIGNER_PYTHON_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    WORKFLOW_DESIGNER_PYTHON_NAME: str
    SENTRY_DSN: Optional[HttpUrl] = None

    @validator("SENTRY_DSN", pre=True)
    def sentry_dsn_can_be_blank(cls, v: str) -> Optional[str]:
        if len(v) == 0:
            return None
        return v

    POSTGRES_SERVER: str
    POSTGRES_SERVER_WORKFLOW_DESIGNER_PYTHON: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB_WORKFLOW_DESIGNER_PYTHON: str
    SQLALCHEMY_DATABASE_URI: Optional[PostgresDsn] = None

    @validator("SQLALCHEMY_DATABASE_URI", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme="postgresql",
            user=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            # host=values.get("POSTGRES_SERVER"),
            host=values.get("POSTGRES_SERVER_WORKFLOW_DESIGNER_PYTHON"),
            path=f"/{values.get('POSTGRES_DB_WORKFLOW_DESIGNER_PYTHON') or ''}",
        )

    FIRST_SUPERUSER: EmailStr
    FIRST_SUPERUSER_PASSWORD: str
    USERS_OPEN_REGISTRATION: bool = False

    class Config:
        case_sensitive = True


settings = Settings()
