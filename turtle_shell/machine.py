from datetime import datetime
import logging

from django_transitions.workflow import StatusBase
from django_transitions.workflow import StateMachineMixinBase


logger = logging.getLogger(__name__)


class ExecutionStatus(StatusBase):
        # Define the statuses as constants
        SOURCE = "="
        CREATED = "created"
        RUNNING = "running"
        DONE = "done"
        ERRORED = "errored"

        STATUS_CHOICES = ((CREATED, "Created execution"),
                          (RUNNING, "Running"),
                          (ERRORED, "Errored"),
                          (DONE, "Completed"))

        # Define the transitions as constants
        CREATE = "create"
        ADVANCE = "advance"
        MARK_COMPLETE = "mark_complete"
        ERROR = "error"

        # The states of the machine
        SM_STATES = [
            dict(name=CREATED, on_enter=[ADVANCE]),
            # Can define more states for the machine like update
            # by overloading ADVANCE to move to next state each time
            dict(name=RUNNING, on_exit=[MARK_COMPLETE]),
            dict(name=ERRORED, on_enter=[ERRORED]),
        ]

        # Define callbacks as constants
        IS_COMPLETE = "is_complete"
        HAS_ERRORED = "has_errored"

        # The machine's initial state
        SM_INITIAL_STATE = CREATED

        # The machine's final states
        SM_FINAL_STATES = [DONE, ERRORED]

        # The transititions as a list of dictionaries
        # This could be defined by classes that extend this functionality based on app-specific functionality
        SM_TRANSITIONS = [
            # reflexive transition to start state machine
            dict(trigger=CREATE, source=CREATED, dest=SOURCE),
            # define how to advance from created to next states
            dict(trigger=ADVANCE, source=CREATED, dest=RUNNING),
            dict(trigger=ADVANCE, source=RUNNING, dest=DONE, conditions=[IS_COMPLETE]),
            # define how to move to errored state
            dict(trigger=ERROR, source=CREATED, dest=ERRORED, conditions=[HAS_ERRORED]),
            dict(trigger=ERROR, source=RUNNING, dest=ERRORED, conditions=[HAS_ERRORED]),
        ]

        # Give the transitions a human readable label and css class
        # which will be used in the django admin
        TRANSITION_LABELS = {
            CREATE: {
                "label": "Transition to start the state machine",
                "cssclass": "default",
            },
            ADVANCE: {"label": "Advance to next state",
                      "cssclass": "default"
                      },
            MARK_COMPLETE: {"label": "Mark an execution when done",
                            "cssclass": "default"
                            },
            ERROR: {"label": "Handle error",
                    "cssclass": "default"
                    },
        }


class FunctionExecutionStateMachineMixin(StateMachineMixinBase):
    status_class = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        assert hasattr(self, "status"), "Subclassers must include a 'status' field."
        assert hasattr(
            self, "status_modified_at"
        ), "Subclassers must include a 'status_modified_at' field."
        assert hasattr(self, "status_history"), "Subclassers must include a 'status_history' field."

    @property
    def state(self):
        """Get the items workflowstate or the initial state if none is set."""
        return self.status

    @state.setter
    def state(self, value):
        """Set the items workflow state."""
        self.status = value
        return self.status

    def track_state_changes(self, *args, **kwargs):
        """Run this on all transitions."""
        logger.debug(
            f"Tracking state changes: {self.uuid}, adding {self.status} to history, {self.status_history}"
        )
        self.status_modified_at = datetime.now()
        self.status_history.append(
            (self.status, self.status_modified_at.strftime("%Y-%m-%d %H:%M:%S (%Z)"))
        )
        self.save()

    def advance(self):
        result = None
        try:
            if self.status not in ExecutionStatus.SM_FINAL_STATES:
                result = self.objects.execute()
        except Exception as exp:
            import traceback
            logger.error(
                f"Failed to execute {self.func_name} :(: {type(exp).__name__}:{exp}", exc_info=True
            )
            error_details = {'type': type(exp).__name__,
                             'message': str(exp),
                             'traceback': traceback.format_exc(), }
            return self.handle_error_response(error_details)
        return result