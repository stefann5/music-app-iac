# music_app_cdk/config.py
from dataclasses import dataclass
from typing import List

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

def get_app_config() -> AppConfig:
    """
    Configuration for the Music App infrastructure
    Optimized for development and learning
    """
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
        enable_x_ray_tracing=False  # Keep costs low
    )