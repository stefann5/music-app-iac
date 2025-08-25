# music_app_cdk/config.py
from dataclasses import dataclass
from typing import List
import os

@dataclass
class AppConfig:
    app_name: str
    removal_policy: str
    password_min_length: int
    lambda_timeout: int
    lambda_memory: int
    cors_origins: List[str]
    api_throttle_rate: int
    api_throttle_burst: int
    enable_detailed_monitoring: bool
    enable_x_ray_tracing: bool

    account: str
    region: str
    max_file_size: int
    allowed_file_types: List[str]

    @property
    def music_bucket_name(self) -> str:
        return f"{self.app_name.lower()}-music-files-{self.account}"

def get_app_config() -> AppConfig:
    """
    Configuration for the Music App infrastructure
    Optimized for development and learning
    """

    account = os.environ.get('CDK_DEFAULT_ACCOUNT', 'dev-account')
    region = os.environ.get('CDK_DEFAULT_REGION', 'eu-central-1')

    return AppConfig(
        app_name='MusicApp',
        removal_policy='DESTROY',  # Easy cleanup when needed
        password_min_length=8,
        lambda_timeout=30,
        lambda_memory=256,
        cors_origins=['*'],  # Allow all origins for easier testing
        api_throttle_rate=100,
        api_throttle_burst=200,
        enable_detailed_monitoring=False,  # Keep costs low
        enable_x_ray_tracing=False,  # Keep costs low

        account=account,
        region=region,
        max_file_size=10 * 1024 * 1024,  # 10 MB
        allowed_file_types=[
            'audio/mpeg',        # MP3
            'audio/wav',         # WAV
            'audio/flac',        # FLAC
            'audio/ogg',         # OGG
            'audio/aac',         # AAC
            'audio/mp4'          # M4A
        ]
    )