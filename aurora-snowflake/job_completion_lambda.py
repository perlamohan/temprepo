"""
The Job Completion Lambda function serves as the final stage in our ETL pipeline, ensuring proper closure of jobs and comprehensive reporting. Here's a breakdown of its key capabilities:

Job Finalization:

Analyzes processing results to determine the final job status (completed, failed, or partial success)
Updates the job status records in DynamoDB with finalization details
Creates historical records for long-term tracking and analysis


Comprehensive Reporting:

Generates detailed job reports with key metrics and performance indicators
Includes processing duration, success rates, and file counts
Provides error analysis with categorization and sampling of error records
Stores reports in S3 for persistent access and sharing


Notification System:

Sends formatted completion notifications via SNS
Includes summary metrics and direct links to detailed reports
Customizes messages based on job outcome (success, failure, partial success)


Automatic Retry Mechanism:

Evaluates failed jobs against configurable retry criteria
Tracks retry attempts to enforce maximum retry limits
Initiates Step Functions workflows for retry operations
Focuses retries specifically on failed files to optimize processing


Audit Trail Integration:

Analyzes audit records to extract timing information
Calculates accurate processing durations
Maintains complete history of job executions for compliance and analysis



The function handles various completion scenarios:

Full Success: All files processed successfully
Complete Failure: All files failed processing
Partial Success: Some files succeeded, some failed
Retry Required: Failed files need reprocessing

This component completes our ETL pipeline by providing:

Clear visibility into job outcomes
Comprehensive documentation for auditing and troubleshooting
Intelligent retry capabilities to maximize data processing success
Long-term historical tracking for performance analysis

The architecture ensures all job execution details are preserved both for immediate operational needs and longer-term analysis.

"""


import json
import os
import boto3
import logging
from datetime import datetime
import traceback
from decimal import Decimal
import uuid

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sns = boto3.client('sns')
step_functions = boto3.client('stepfunctions')

# Get table references
audit_table = dynamodb.Table(os.environ['AUDIT_TABLE_NAME'])
job_config_table = dynamodb.Table(os.environ['JOB_CONFIG_TABLE_NAME'])
job_status_table = dynamodb.Table(os.environ['JOB_STATUS_TABLE_NAME'])
error_table = dynamodb.Table(os.environ['ERROR_TABLE_NAME']) 
job_history_table = dynamodb.Table(os.environ['JOB_HISTORY_TABLE_NAME'])

# SNS Topic for notifications
sns_topic_arn = os.environ['SNS_TOPIC_ARN']
reports_bucket = os.environ['REPORTS_BUCKET']

class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert Decimal objects to float for JSON serialization"""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def get_job_config(job_id):
    """Retrieve job configuration from DynamoDB"""
    try:
        response = job_config_table.get_item(Key={'job_id': job_id})
        return response.get('Item')
    except Exception as e:
        logger.error(f"Error retrieving job config for job_id {job_id}: {str(e)}")
        raise

def get_job_status(job_id, execution_id):
    """Retrieve current job status from DynamoDB"""
    try:
        response = job_status_table.get_item(
            Key={
                'job_id': job_id,
                'execution_id': execution_id
            }
        )
        return response.get('Item')
    except Exception as e:
        logger.error(f"Error retrieving job status for job_id {job_id}, execution_id {execution_id}: {str(e)}")
        raise

def get_error_details(job_id, execution_id):
    """Retrieve all errors for a job execution"""
    try:
        response = error_table.query(
            IndexName='JobExecutionIndex',
            KeyConditionExpression='job_id = :job_id AND begins_with(audit_id, :execution_id)',
            ExpressionAttributeValues={
                ':job_id': job_id,
                ':execution_id': execution_id
            }
        )
        return response.get('Items', [])
    except Exception as e:
        logger.error(f"Error retrieving errors for job_id {job_id}, execution_id {execution_id}: {str(e)}")
        return []

def get_audit_records(job_id, execution_id):
    """Retrieve all audit records for a job execution"""
    try:
        response = audit_table.query(
            IndexName='JobExecutionIndex',
            KeyConditionExpression='job_id = :job_id AND begins_with(audit_id, :execution_id)',
            ExpressionAttributeValues={
                ':job_id': job_id,
                ':execution_id': execution_id
            }
        )
        return response.get('Items', [])
    except Exception as e:
        logger.error(f"Error retrieving audit records for job_id {job_id}, execution_id {execution_id}: {str(e)}")
        return []

def update_job_status(job_id, execution_id, status):
    """Update job status in DynamoDB"""
    try:
        job_status_table.update_item(
            Key={
                'job_id': job_id,
                'execution_id': execution_id
            },
            UpdateExpression="SET #status = :status, #completed_at = :completed_at, #last_updated = :last_updated",
            ExpressionAttributeNames={
                '#status': 'status',
                '#completed_at': 'completed_at',
                '#last_updated': 'last_updated'
            },
            ExpressionAttributeValues={
                ':status': status,
                ':completed_at': datetime.now().isoformat(),
                ':last_updated': datetime.now().isoformat()
            }
        )
    except Exception as e:
        logger.error(f"Error updating job status: {str(e)}")
        raise

def create_job_history_record(job_id, execution_id, status, metrics):
    """Create a job history record in DynamoDB"""
    try:
        timestamp = datetime.now().isoformat()
        history_id = str(uuid.uuid4())
        
        history_item = {
            'history_id': history_id,
            'job_id': job_id,
            'execution_id': execution_id,
            'status': status,
            'metrics': metrics,
            'timestamp': timestamp
        }
        
        job_history_table.put_item(Item=history_item)
        return history_id
    except Exception as e:
        logger.error(f"Error creating job history record: {str(e)}")
        return None

def generate_job_report(job_id, execution_id, job_config, job_status, errors, audit_records):
    """Generate a comprehensive job report"""
    try:
        # Basic job information
        job_name = job_config.get('job_name', 'Unknown Job')
        source_type = job_config.get('source_type', 'Unknown')
        target_type = job_config.get('target_type', 'Unknown')
        
        # Extract timestamps
        start_time = None
        end_time = None
        for record in audit_records:
            if record.get('status') == 'JOB_STARTED':
                start_time = record.get('created_at')
            if record.get('status') in ['JOB_COMPLETED', 'JOB_FAILED', 'JOB_COMPLETED_WITH_ERRORS']:
                end_time = record.get('updated_at')
        
        # If specific timestamps are not found, use ones from job_status
        if not start_time:
            start_time = job_status.get('created_at')
        if not end_time:
            end_time = job_status.get('completed_at', datetime.now().isoformat())
        
        # Calculate duration if both timestamps are available
        duration_seconds = None
        if start_time and end_time:
            try:
                start_dt = datetime.fromisoformat(start_time)
                end_dt = datetime.fromisoformat(end_time)
                duration_seconds = (end_dt - start_dt).total_seconds()
            except:
                duration_seconds = None
        
        # Compile metrics
        metrics = {
            'total_files': job_status.get('total_files', 0),
            'files_processed': job_status.get('files_processed', 0),
            'files_succeeded': job_status.get('files_succeeded', 0),
            'files_failed': job_status.get('files_failed', 0),
            'success_rate': job_status.get('success_rate', 0),
            'duration_seconds': duration_seconds
        }
        
        # Error summary
        error_summary = []
        error_types = {}
        for error in errors:
            error_type = error.get('error_type', 'Unknown')
            if error_type in error_types:
                error_types[error_type] += 1
            else:
                error_types[error_type] = 1
                
            # Include first 100 errors in detail
            if len(error_summary) < 100:
                error_summary.append({
                    'error_id': error.get('error_id'),
                    'file_key': error.get('file_key'),
                    'error_type': error_type,
                    'error_message': error.get('error_message'),
                    'timestamp': error.get('timestamp')
                })
        
        # Construct the report
        report = {
            'job_id': job_id,
            'execution_id': execution_id,
            'job_name': job_name,
            'status': job_status.get('status', 'Unknown'),
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': duration_seconds,
            'source_type': source_type,
            'target_type': target_type,
            'metrics': metrics,
            'error_types': error_types,
            'error_count': len(errors),
            'error_sample': error_summary if errors else [],
            'generated_at': datetime.now().isoformat()
        }
        
        # Save report to S3
        report_key = f"reports/{job_id}/{execution_id}/job_report.json"
        s3_client.put_object(
            Bucket=reports_bucket,
            Key=report_key,
            Body=json.dumps(report, cls=DecimalEncoder),
            ContentType='application/json'
        )
        
        return {
            'report_url': f"s3://{reports_bucket}/{report_key}",
            'report': report
        }
    except Exception as e:
        logger.error(f"Error generating job report: {str(e)}\n{traceback.format_exc()}")
        return {
            'report_url': None,
            'error': str(e)
        }

def send_completion_notification(job_id, execution_id, status, job_name, metrics, report_url):
    """Send detailed completion notification via SNS"""
    try:
        subject = f"ETL Job {status}: {job_name} ({job_id})"
        
        # Format metrics for readability
        metrics_str = "\n".join([
            f"Total files: {metrics.get('total_files', 0)}",
            f"Files processed: {metrics.get('files_processed', 0)}",
            f"Files succeeded: {metrics.get('files_succeeded', 0)}",
            f"Files failed: {metrics.get('files_failed', 0)}",
            f"Success rate: {metrics.get('success_rate', 0)}%",
        ])
        
        # Add duration if available
        duration_seconds = metrics.get('duration_seconds')
        if duration_seconds:
            minutes, seconds = divmod(duration_seconds, 60)
            hours, minutes = divmod(minutes, 60)
            duration_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            metrics_str += f"\nDuration: {duration_str}"
        
        message = (
            f"ETL Job Completion Report\n"
            f"========================\n\n"
            f"Job Name: {job_name}\n"
            f"Job ID: {job_id}\n"
            f"Execution ID: {execution_id}\n"
            f"Status: {status}\n\n"
            f"Performance Metrics:\n"
            f"{metrics_str}\n\n"
            f"Detailed report available at:\n{report_url}\n\n"
            f"This is an automated notification from the ETL pipeline."
        )
        
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=message
        )
        
        return True
    except Exception as e:
        logger.error(f"Error sending completion notification: {str(e)}")
        return False

def handle_retries(job_id, execution_id, job_config, failed_count):
    """Handle automatic retry logic if configured"""
    try:
        # Check if retry is configured
        retry_config = job_config.get('retry_config')
        if not retry_config:
            return None
        
        # Check if automatic retry is enabled
        auto_retry = retry_config.get('auto_retry', False)
        if not auto_retry:
            return None
            
        # Check if max retries not exceeded
        max_retries = retry_config.get('max_retries', 3)
        current_attempt = job_config.get('current_attempt', 1)
        
        if current_attempt >= max_retries:
            logger.info(f"Maximum retry attempts ({max_retries}) reached for job {job_id}")
            return None
            
        # Check if failure threshold is met
        retry_threshold = retry_config.get('failure_threshold_percent', 10)
        job_status = get_job_status(job_id, execution_id)
        total_files = job_status.get('total_files', 0)
        
        if total_files == 0:
            return None
            
        failure_percent = (failed_count / total_files) * 100
        
        if failure_percent < retry_threshold:
            logger.info(f"Failure rate {failure_percent}% is below threshold {retry_threshold}% for job {job_id}")
            return None
            
        # All conditions met for retry
        # Prepare for retry by updating job config with retry attempt
        job_config_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET current_attempt = :attempt",
            ExpressionAttributeValues={
                ':attempt': current_attempt + 1
            }
        )
        
        # Start a new execution focusing only on failed files
        # This would typically involve starting a Step Function execution
        # that only processes the failed files from the previous run
        logger.info(f"Initiating retry (attempt {current_attempt + 1} of {max_retries}) for job {job_id}")
        
        # Get the Step Function ARN from environment variables
        retry_step_function_arn = os.environ.get('RETRY_STEP_FUNCTION_ARN')
        
        if not retry_step_function_arn:
            logger.error("RETRY_STEP_FUNCTION_ARN not set in environment variables")
            return None
            
        # Start the retry workflow
        response = step_functions.start_execution(
            stateMachineArn=retry_step_function_arn,
            name=f"{job_id}-retry-{current_attempt + 1}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            input=json.dumps({
                'job_id': job_id,
                'original_execution_id': execution_id,
                'retry_attempt': current_attempt + 1,
                'max_retries': max_retries
            })
        )
        
        return {
            'retry_initiated': True,
            'retry_attempt': current_attempt + 1,
            'max_retries': max_retries,
            'execution_arn': response['executionArn']
        }
        
    except Exception as e:
        logger.error(f"Error handling retries: {str(e)}\n{traceback.format_exc()}")
        return None

def lambda_handler(event, context):
    """Main Lambda handler function"""
    try:
        # Extract parameters from event
        job_id = event['job_id']
        execution_id = event['execution_id']
        
        logger.info(f"Processing job completion for job_id: {job_id}, execution_id: {execution_id}")
        
        # Get job configuration and status
        job_config = get_job_config(job_id)
        job_status = get_job_status(job_id, execution_id)
        
        if not job_config or not job_status:
            error_message = f"Job configuration or status not found for job_id: {job_id}, execution_id: {execution_id}"
            logger.error(error_message)
            return {
                'statusCode': 404,
                'body': json.dumps({'error': error_message})
            }
        
        # Get errors and audit records
        errors = get_error_details(job_id, execution_id)
        audit_records = get_audit_records(job_id, execution_id)
        
        # Extract key metrics
        total_files = job_status.get('total_files', 0)
        succeeded_files = job_status.get('files_succeeded', 0)
        failed_files = job_status.get('files_failed', 0)
        
        # Determine final status
        status = job_status.get('status')
        if not status or status == 'IN_PROGRESS':
            if failed_files == 0 and succeeded_files == total_files:
                status = 'JOB_COMPLETED'
            elif failed_files == total_files:
                status = 'JOB_FAILED'
            elif succeeded_files + failed_files == total_files:
                status = 'JOB_COMPLETED_WITH_ERRORS'
            else:
                status = 'IN_PROGRESS'
                
        # Update job status
        update_job_status(job_id, execution_id, status)
        
        # Generate metrics for reporting
        metrics = {
            'total_files': total_files,
            'files_processed': succeeded_files + failed_files,
            'files_succeeded': succeeded_files,
            'files_failed': failed_files,
            'success_rate': job_status.get('success_rate', 0),
            'duration_seconds': None  # Will be calculated in generate_job_report
        }
        
        # Create job history record
        history_id = create_job_history_record(job_id, execution_id, status, metrics)
        
        # Generate comprehensive job report
        report_result = generate_job_report(job_id, execution_id, job_config, job_status, errors, audit_records)
        report_url = report_result.get('report_url')
        report = report_result.get('report')
        
        # Check if retry is needed for failed files
        retry_result = None
        if failed_files > 0 and status in ['JOB_FAILED', 'JOB_COMPLETED_WITH_ERRORS']:
            retry_result = handle_retries(job_id, execution_id, job_config, failed_files)
        
        # Send notification
        job_name = job_config.get('job_name', 'Unknown Job')
        notification_sent = send_completion_notification(
            job_id, execution_id, status, job_name, 
            report.get('metrics') if report else metrics, 
            report_url
        )
        
        # Return result
        return {
            'statusCode': 200,
            'body': json.dumps({
                'job_id': job_id,
                'execution_id': execution_id,
                'status': status,
                'metrics': metrics,
                'history_id': history_id,
                'report_url': report_url,
                'notification_sent': notification_sent,
                'retry': retry_result
            }, cls=DecimalEncoder)
        }
        
    except Exception as e:
        error_message = str(e)
        stack_trace = traceback.format_exc()
        logger.error(f"Error in job completion handler: {error_message}\n{stack_trace}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message,
                'stack_trace': stack_trace
            })
        }