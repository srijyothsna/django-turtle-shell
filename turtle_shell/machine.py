from datetime import datetime
import logging

from django_transitions.workflow import StatusBase
from django_transitions.workflow import StateMachineMixinBase

from turtle_shell.tasks import move_to_execute


logger = logging.getLogger(__name__)


class ExecutionStatus(StatusBase):
        # Define the statuses as constants
        SOURCE = "="
        CREATED = "CREATED"
        RUNNING = "RUNNING"
        DONE = "DONE"
        ERRORED = "ERRORED"

        STATUS_CHOICES = ((CREATED, "Created execution"),
                          (RUNNING, "Running"),
                          (ERRORED, "Errored"),
                          (DONE, "Completed"))

        # Define the transitions as constants
        CREATE = "create"
        ADVANCE = "advance"
        ERROR = "error"

        # The states of the machine
        SM_STATES = [
            dict(name=CREATED, on_enter=[ADVANCE]),
            # Can define more states for the machine like update
            # by overloading ADVANCE to move to next state each time
            dict(name=RUNNING, on_exit=[ADVANCE]),
            dict(name=ERRORED, on_enter=[ERRORED]),
        ]

        # Define conditions as constants
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
            #dict(trigger=CREATE, source=CREATED, dest=SOURCE),
            dict(trigger=CREATE, source=SOURCE, dest=CREATED),
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
        logger.debug(f"In advance(): {self.func_name} for {self.uuid} has status {self.status}")
        result = None
        try:
            if self.is_pending:
                execute_task = move_to_execute.delay(self.uuid)
                print(f"execute_task:: {execute_task}")
                logger.info(f"******** execute_task:: {execute_task} ************")
        except Exception as ex:
            import traceback
            logger.error(
                f"Failed to advance {self.uuid} from {self.status} status :(: {type(ex).__name__}:{ex}", exc_info=True
            )
            error_details = {'type': type(ex).__name__,
                             'message': str(ex),
                             'traceback': traceback.format_exc(), }
            error_response = self.handle_error_response(error_details)
            raise Exception(f"Failed on {self.func_name}\n Error Response:: {error_response}", ex) from ex
        logger.debug(f"Returning from advance(): {self.func_name} for {self.uuid} with\n {result}")
        return result
