# music_app_cdk/config.py
from dataclasses import dataclass

@dataclass
class RegistrationConfig:
    stage: str
    app_name: str
    removal_policy: str
    password_min_length: int
    lambda_timeout: int
    lambda_memory: int
    cors_origins: list

def get_registration_config(stage: str) -> RegistrationConfig:
    """
    Imperative configuration based on stage
    """
    if stage == 'dev':
        return RegistrationConfig(
            stage=stage,
            app_name='MusicApp',
            removal_policy='DESTROY',  # Easy cleanup in dev
            password_min_length=8,
            lambda_timeout=30,
            lambda_memory=256,
            cors_origins=['*']  # Allow all in dev
        )
    elif stage == 'prod':
        return RegistrationConfig(
            stage=stage,
            app_name='MusicApp', 
            removal_policy='RETAIN',  # Keep data in prod
            password_min_length=12,  # Stronger passwords
            lambda_timeout=60,
            lambda_memory=512,
            cors_origins=['https://yourdomain.com']  # Restrict in prod
        )
    else:
        raise ValueError(f"Unknown stage: {stage}")