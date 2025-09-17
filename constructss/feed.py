from aws_cdk import Duration, aws_sqs as sqs
from constructs import Construct

from config import AppConfig

class FeedConstruct(Construct):
    """Feed infrastructure for song text processing"""
    
    def __init__(self, scope: Construct, id: str, config: AppConfig):
        super().__init__(scope, id)
        
        self.config = config
        
        print(f"Creating feed SQS queue...")
        self.feed_queue = self._create_feed_queue()
    
    def _create_feed_queue(self) -> sqs.Queue:
        """SQS Queue for async feed processing with DLQ"""
        
        # Dead Letter Queue for failed processing
        dlq = sqs.Queue(
            self,
            "FeedDLQ",
            queue_name=f"{self.config.app_name}-FeedDLQ",
            retention_period=Duration.days(14)
        )
        
        # Main transcription queue
        queue = sqs.Queue(
            self,
            "FeedQueue",
            queue_name=f"{self.config.app_name}-Feed",
            visibility_timeout=Duration.minutes(15),  # Max transcription time
            receive_message_wait_time=Duration.seconds(20),  # Long polling for efficiency
            retention_period=Duration.days(7),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq
            )
        )
        
        print("Feed queue created with DLQ")
        return queue