from celery import shared_task
from celery.utils.log import get_task_logger

from turtle_shell.models import ExecutionResult
from turtle_shell.machine import ExecutionStatus


logger = get_task_logger(__name__)


@shared_task()
def advance_executions():
    pending_executions = ExecutionResult.objects.exclude(status__in=ExecutionStatus.SM_FINAL_STATES)
    logger.info(f"{pending_executions.count()} pending function execution Submissions...")
    for pending_execution in pending_executions:
        logger.info(f"Advancing {pending_execution.uuid}")
        # while it is not completed, continue, keep checking if done
        result = pending_execution.advance()


@shared_task()
def move_to_execute(uuid):
    execution = ExecutionResult.objects.filter(uuid=uuid)
    logger.info(f"Moving execution {execution.uuid} for {execution.func_name} to execute")
    result = execution.execute()
    execution.save()
