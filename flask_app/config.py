from dataclasses import dataclass, field
import os

@dataclass
class RedshiftConfig:
    host: str = os.getenv("REDSHIFT_HOST", "")
    port: int = int(os.getenv("REDSHIFT_PORT", "5439"))
    dbname: str = os.getenv("REDSHIFT_DBNAME", "")
    user: str = os.getenv("REDSHIFT_USER", "")
    password: str = os.getenv("REDSHIFT_PASSWORD", "")

@dataclass
class AWSConfig:
    access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    bucket_name: str = os.getenv("S3_BUCKET_NAME", "static.wowcher.co.uk")

@dataclass
class OracleConfig:
    user: str = os.getenv("ORACLE_USER", "")
    password: str = os.getenv("ORACLE_PASSWORD", "")
    dsn: str = os.getenv("ORACLE_DSN", "")

@dataclass
class OpenAIConfig:
    api_key: str = os.getenv("OPEN_AI_API_KEY", "")

@dataclass
class AppConfig:
    redshift: RedshiftConfig = field(default_factory=RedshiftConfig)
    aws: AWSConfig = field(default_factory=AWSConfig)
    oracle: OracleConfig = field(default_factory=OracleConfig)
    openai: OpenAIConfig = field(default_factory=OpenAIConfig)
    batch_name: str = os.getenv("BATCH_NAME", "OPEN AI Images")
