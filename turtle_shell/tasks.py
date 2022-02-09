from celery import shared_task
from celery.utils.log import get_task_logger

from turtle_shell.models import ExecutionResult


logger = get_task_logger(__name__)

"""
@shared_task()
def advance_executions():
    pending_executions = ExecutionResult.objects.pending()
    logger.info(f"{pending_executions.count()} pending function execution Submissions...")
    for pending_execution in pending_executions:
        logger.info(f"Advancing {pending_execution.id}")
        # while it is not completed, continue, keep checking if done
        result = pending_execution.advance()
"""