import json
import logging
import sys
import uuid

from django.db import models, transaction
from django.urls import reverse
from django.conf import settings
from transitions import Machine

from turtle_shell import utils
from turtle_shell.machine import ExecutionStatus
from turtle_shell.machine import FunctionExecutionStateMachineMixin


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


class CaughtException(Exception):
    """An exception that was caught and saved. Generally don't need to rollback transaction with
    this one :)"""

    def __init__(self, exc, message):
        self.exc = exc
        super().__init__(message)


class ResultJSONEncodeException(CaughtException):
    """Exceptions for when we cannot save result as actual JSON field :("""


class CreationException(CaughtException):
    """Exception for when an execution cannot be created """


class ValidationException(CaughtException):
    """Exception for when execution input validation fails"""


class ExecutionException(CaughtException):
    """Exception for when execution fails"""


class ExecutionManager(models.Manager):
    def get_execution_by_uuid(self, uuid):
        return ExecutionResult.objects.filter(uuid=uuid)


class Execution():
    def handle_error_response(self, error_details):
        error_response = {}
        error_response['uuid'] = self.uuid
        error_response['error_details'] = error_details
        self.traceback = error_details['traceback']
        self.error_json = {"type": error_details['type'], "message": error_details['message']}
        with transaction.atomic():
            self.save()
        return error_response

    def validate_execution_input(self, uuid, func, input_json):
        # Override to define app-specific input validation for function
        # Raise ValidationException
        pass

    def _validate_inputs(self, input, func_name):
        try:
            self.validate_execution_input(input['uuid'], func_name, input['input_json'])
            # Can be overridden to define app-specific input validation for function executions
        except ValidationException as ve:
            import traceback
            logger.error(
                f"Failed to validate inputs for {func_name} :(: {type(ve).__name__}:{ve}",
                exc_info=True
            )
            # TODO: catch integrity error separately
            error_details = {'error_type': type(ve).__name__,
                             'message': str(ve),
                             'error_traceback': "".join(traceback.format_exc())}
            error_response = self.handle_error_response(error_details)
            raise CaughtException(f"Failed on {func_name}\n Error Response:: {error_response}", ve) from ve
        return self.get_current_state()

    def create(self, **kwargs):
        logger.info("In create(): Creating an execution")
        print("In create(): Creating an execution")
        try:
            func = self.get_function()
            # Here the execution instance is created, so the
            cur_inp = self.get_current_state()
            val_inp = self._validate_inputs(cur_inp, self.func_name)
            with transaction.atomic():
                self.save()
        except CreationException as ex:
            import traceback
            logger.error(
                f"Failed to create {self.func_name} :(: {type(ex).__name__}:{ex}", exc_info=True
            )
            error_details = {'type': type(ex).__name__,
                             'message': str(ex),
                             'traceback': "".join(traceback.format_exc()),}
            error_response = self.handle_error_response(error_details)
            raise CaughtException(f"Failed on {self.func_name}\n Error Response:: {error_response}", ex) from ex
        logger.info(f"In create(): Created an execution {val_inp}")
        print(f"In create(): Created an execution {val_inp}")
        return val_inp

    def execute(self, **kwargs):
        logger.debug("In execute(): Executing a function")
        original_result = None
        try:
            func = self.get_function()
            result = original_result = func(**self.input_json)
        except ExecutionException as ex:
            import traceback
            logger.error(
                f"Failed to execute {self.func_name} :(: {type(ex).__name__}:{ex}", exc_info=True
            )
            # TODO: catch integrity error separately
            error_details = {'error_type': type(ex).__name__,
                             'message': str(ex),
                             'error_traceback': "".join(traceback.format_exc())}
            error_response = self.handle_error_response(error_details)
            raise CaughtException(f"Failed on {self.func_name}\n Error Response:: {error_response}", ex) from ex
        try:
            if hasattr(result, "json"):
                result = json.loads(result.json())
                self.output_json = result
                # allow ourselves to save again externally
                with transaction.atomic():
                    self.save()
        except TypeError as e:
            import traceback
            self.error_json = {"type": type(e).__name__, "message": str(e)}
            self.traceback = "".join(traceback.format_exc())
            msg = f"Failed on {self.func_name} ({type(e).__name__})"

            if "JSON serializable" in str(e):
                # save it as a str so we can at least have something to show
                self.output_json = str(result)
                self.save()
                raise ResultJSONEncodeException(msg, e) from e
            else:
                raise e
        logger.debug(f"In execute(): Executed a function:: {self.func_name} for {self.uuid}")
        return original_result

    def mark_complete(self):
        logger.debug(f"In mark_complete(): Marking a function complete:: {self.func_name} for {self.uuid}")
        result = None
        try:
            if self.is_complete:
                result = self.get_current_state()
                self.save()
        except ExecutionException as ex:
            import traceback
            logger.error(
                f"Failed to mark {self.func_name} as completed:(: {type(ex).__name__}:{ex}", exc_info=True
            )
            # TODO: catch integrity error separately
            error_details = {'error_type': type(ex).__name__,
                             'message': str(ex),
                             'error_traceback': "".join(traceback.format_exc())}
            error_response = self.handle_error_response(error_details)
            raise CaughtException(f"Failed on {self.func_name}\n Error Response:: {error_response}", ex) from ex
        logger.debug(f"In mark_complete():Marked function complete:: {self.func_name} for {self.uuid}")
        return result


class ExecutionResult(FunctionExecutionStateMachineMixin, Execution, models.Model):
    FIELDS_TO_SHOW_IN_LIST = [
        ("func_name", "Function"),
        ("created", "Created"),
        ("user", "User"),
        ("status", "Status"),
        ("modified", "Modified")
    ]
    uuid = models.UUIDField(primary_key=True, unique=True, editable=False, default=uuid.uuid4)
    func_name = models.CharField(max_length=512, editable=False)
    input_json = models.JSONField(encoder=utils.EnumAwareEncoder, decoder=utils.EnumAwareDecoder)
    output_json = models.JSONField(
        default=dict, null=True, encoder=utils.EnumAwareEncoder, decoder=utils.EnumAwareDecoder
    )
    error_json = models.JSONField(
        default=dict, null=True, encoder=utils.EnumAwareEncoder, decoder=utils.EnumAwareDecoder
    )
    traceback = models.TextField(default="")
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True)

    status = models.CharField(
        max_length=50,
        choices=ExecutionStatus.STATE_CHOICES,
        default=ExecutionStatus.SM_INITIAL_STATE,
    )
    status_modified_at = models.DateTimeField(auto_now_add=True)
    status_history = models.JSONField(default=list)
    # state machine setup
    status_class = ExecutionStatus
    machine = Machine(
        model=None,
        auto_transitions=False,
        queued=True,
        after_state_change="track_state_changes",
        **status_class.get_kwargs(),  # noqa: C815
    )

    # Manager for the model object
    objects = ExecutionManager()

    def get_function(self):
        # TODO: figure this out
        from . import get_registry

        func_obj = get_registry().get(self.func_name)
        if not func_obj:
            raise ValueError(f"No registered function defined for {self.func_name}")
        return func_obj.func

    @property
    def is_complete(self):
        # The execution result is saved to output_json only upon execution completion
        if self.output_json:
            logger.debug(f"{self.func_name} for {self.uuid} is complete. Returning True for is_complete()")
            return True
        return False

    @property
    def has_errored(self):
        if self.error_json:
            logger.debug(f"{self.func_name} for {self.uuid} is complete. Returning True for has_errored()")
            return True
        return False

    @property
    def is_pending(self):
        if self.status in ExecutionStatus.SM_FINAL_STATES:
            return False
        logger.debug(f"{self.func_name} for {self.uuid} is pending. Returning True for is_pending()")
        return True


    #TO-DO: Define is_cancelled and logic for tracking cancellations

    def get_absolute_url(self):
        # TODO: prob better way to do this so that it all redirects right :(
        return reverse(f"turtle_shell:detail-{self.func_name}", kwargs={"pk": self.pk})

    def __repr__(self):
        return f"<{type(self).__name__}({self})"

    @property
    def pydantic_object(self):
        from turtle_shell import pydantic_adapter

        return pydantic_adapter.get_pydantic_object(self)

    @property
    def list_entry(self) -> list:
        return [getattr(self, obj_name) for obj_name, _ in self.FIELDS_TO_SHOW_IN_LIST]

    def get_current_state(self):
        # Override this to define app-specific behavior
        # to calculate internal state of inputs (based on output from previous task),
        # the status and return the new internal state of inputs (or output at this stage) and new status.
        # This can be defined by apps extending this functionality.
        return {
            'function_name': self.func_name,
            'input_json': self.input_json,
            'uuid': self.uuid,
            'status': self.status,
            'status_history': self.status_history,  # starting status could be None and could pass next possible state
            'status_modified_at': self.status_modified_at,
            'output_json': self.output_json  # None until output is ready
        }
