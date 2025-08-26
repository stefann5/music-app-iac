from aws_cdk import (
    aws_s3 as s3,
    RemovalPolicy,
    Duration
)
from constructs import Construct
from config import AppConfig

class S3Construct(Construct):
    def __init__(self, scope: Construct, id: str, config: AppConfig):
        super().__init__(scope, id)

        self.config = config

        print(f"Creating S3 bucket for music files...")

        self.music_bucket = self._create_music_bucket()
    
    def _create_music_bucket(self) -> s3.Bucket:
        bucket = s3.Bucket(
            self,
            "MusicBucket",
            bucket_name=self.config.music_bucket_name,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteIncompleteMultipartUploads",
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    enabled=True
                )
            ],

            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.DELETE,
                        s3.HttpMethods.HEAD
                    ],
                    allowed_origins=self.config.cors_origins,
                    allowed_headers=['*'],
                    max_age=3000
                )
            ],

            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        print(f"S3 bucket '{bucket.bucket_name}' created.")
        return bucket