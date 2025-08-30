from aws_cdk import Duration, aws_sqs as sqs
from constructs import Construct

from config import AppConfig

class TranscriptionConstruct(Construct):
    """Transcription infrastructure for song text processing"""
    
    def __init__(self, scope: Construct, id: str, config: AppConfig):
        super().__init__(scope, id)
        
        self.config = config
        
        print(f"Creating transcription SQS queue...")
        self.transcription_queue = self._create_transcription_queue()
    
    def _create_transcription_queue(self) -> sqs.Queue:
        """SQS Queue for async transcription processing with DLQ"""
        
        # Dead Letter Queue for failed processing
        dlq = sqs.Queue(
            self,
            "TranscriptionDLQ",
            queue_name=f"{self.config.app_name}-TranscriptionDLQ",
            retention_period=Duration.days(14)
        )
        
        # Main transcription queue
        queue = sqs.Queue(
            self,
            "TranscriptionQueue",
            queue_name=f"{self.config.app_name}-Transcription",
            visibility_timeout=Duration.minutes(15),  # Max transcription time
            receive_message_wait_time=Duration.seconds(20),  # Long polling for efficiency
            retention_period=Duration.days(7),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq
            )
        )
        
        print("Transcription queue created with DLQ")
        return queue