import uuid
import json
import logging

from django.db import models, transaction
from django.urls import reverse
from django.conf import settings
from transitions import Machine

from turtle_shell import utils
from turtle_shell.machine import ExecutionStatus
from turtle_shell.machine import FunctionExecutionStateMachineMixin


logger = logging.getLogger(__name__)


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


class Execution(models.Manager):
    def pending(self):
        return ExecutionResult.objects.exclude(status__in=ExecutionStatus.SM_FINAL_STATES)

    def advance(self) -> bool:
        try:
            if not self.object.status == (ExecutionStatus.RUNNING or ExecutionStatus.SM_FINAL_STATES):
                result = self.object.execute()
        except Exception as exp:
            import traceback
            logger.error(
                f"Failed to execute {self.object.func_name} :(: {type(exp).__name__}:{exp}", exc_info=True
            )
            error_details = {'type': type(exp).__name__,
                             'message': str(exp),
                             'traceback': traceback.format_exc(), }
            return self.handle_error_response(error_details)
        return result

    def get_current_state(self):
        # Override this to define app-specific behavior
        # to calculate internal state of inputs (based on output from previous task),
        # the status and return the new internal state of inputs (or output at this stage) and new status.
        # This can be defined by apps extending this functionality.
        return {
            'function_name': self.object.func_name,
            'input_json': self.object.input_json,
            'uuid': self.object.uuid,
            'status': self.object.status,
            'status_history': self.object.status_history,  # starting status could be None and could pass next possible state
            'status_modified_at': self.object.status_modified_at,
            'output_json': self.object.output_json  # None until output is ready
        }

    def handle_error_response(self, error_details):
        error_response = {}
        error_response['uuid'] = self.object.uuid
        error_response['error_details'] = error_details
        self.object.traceback = error_details['traceback']
        self.object.error_json = {"type": error_details['type'], "message": error_details['message']}
        with transaction.atomic():
            self.object.save()
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
                f"Failed to validate inputs for {self.object.func_name} :(: {type(ve).__name__}:{ve}",
                exc_info=True
            )
            # TODO: catch integrity error separately
            error_details = {'error_type': type(ve).__name__,
                             'message': str(ve),
                             'error_traceback': "".join(traceback.format_exc())}
            error_response = self.handle_error_response(error_details)
            raise CaughtException(f"Failed on {self.object.func_name}\n Error Response:: {error_response}", ve) from ve
        return self.get_current_state()

    def create(self, **kwargs):
        try:
            func = self.object.get_function()
            # Here the execution instance is created, so the
            cur_inp = self.get_current_state()
            val_inp = self._validate_inputs(cur_inp, self.object.func_name)
            with transaction.atomic():
                self.object.save()
        except CreationException as ce:
            import traceback
            logger.error(
                f"Failed to create {self.object.func_name} :(: {type(ce).__name__}:{ce}", exc_info=True
            )
            error_details = {'type': type(ce).__name__,
                             'message': str(ce),
                             'traceback': "".join(traceback.format_exc()),}
            return self.handle_error_response(error_details)
        return val_inp

    def execute(self, **kwargs):
        original_result = None
        try:
            func = self.object.get_function()
            result = original_result = func(**self.object.input_json)
        except ExecutionException as ee:
            import traceback
            logger.error(
                f"Failed to execute {self.object.func_name} :(: {type(ee).__name__}:{ee}", exc_info=True
            )
            # TODO: catch integrity error separately
            error_details = {'error_type': type(ee).__name__,
                             'message': str(ee),
                             'error_traceback': "".join(traceback.format_exc())}
            error_response = self.handle_error_response(error_details)
            raise CaughtException(f"Failed on {self.func_name}\n Error Response:: {error_response}", ee) from ee
        try:
            if hasattr(result, "json"):
                result = json.loads(result.json())
                self.object.output_json = result
                # allow ourselves to save again externally
                with transaction.atomic():
                        self.object.save()
        except TypeError as e:
            import traceback
            self.object.error_json = {"type": type(e).__name__, "message": str(e)}
            self.object.traceback = "".join(traceback.format_exc())
            msg = f"Failed on {self.func_name} ({type(e).__name__})"

            if "JSON serializable" in str(e):
                # save it as a str so we can at least have something to show
                self.object.output_json = str(result)
                self.object.save()
                raise ResultJSONEncodeException(msg, e) from e
            else:
                raise e
        return original_result


class ExecutionResult(FunctionExecutionStateMachineMixin, models.Model):
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

    # use custom manager
    objects = Execution()

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
            return True
        return False

    @property
    def has_errored(self):
        if self.error_json:
            return True
        return False

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
