from __future__ import annotations
import asyncio
import time
from asgiref.sync import sync_to_async
from typing import Any, Sequence, AsyncIterator
from airflow.configuration import conf
from airflow.models.baseoperator import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context


class MyTrigger(BaseTrigger):

    def __init__(
        self,
        poll_interval: float = 5.0,
        my_secret_word_1: str = "notset",
        my_secret_word_2: str = "notset",
    ):
        super().__init__()
        self.poll_interval = poll_interval
        self.my_secret_word_1 = my_secret_word_1
        self.my_secret_word_2 = my_secret_word_2

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize MyTrigger arguments and classpath. All arguments must be serializable."""
        return (
            "include.deferrable_operator_template.MyTrigger",
            {
                "my_secret_word_1": self.my_secret_word_1,
                "my_secret_word_2": self.my_secret_word_2,
            },
        )
        # This is what the trigger will return when it is complete

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            result = await self.my_trigger_function()
            if result == 2:
                self.log.info(f"Result was 2, thats the number! Triggering event.")
                
                self.log.info(f"Old secret word 1: {self.my_secret_word_1}")
                self.log.info(f"Old secret word 2: {self.my_secret_word_2}")

                self.my_secret_word_1 = "banana"
                self.my_secret_word_2 = "apple"

                self.log.info(f"New secret word 1: {self.my_secret_word_1}")
                self.log.info(f"New secret word 2: {self.my_secret_word_2}")

                yield TriggerEvent(self.serialize())
                return
            else:
                self.log.info(
                    f"Result was not the one we are waiting for. Sleeping for {self.poll_interval} seconds."
                )
                await asyncio.sleep(self.poll_interval)

    @sync_to_async
    def my_trigger_function(self) -> str:
        """This is where what you are waiting for goes."""

        import random

        randint = random.choice([0,2])

        self.log.info(f"Random number: {randint}")

        return randint


class MyOperator(BaseOperator):

    template_fields: Sequence[str] = (
        "wait_for_completion",
        "poke_interval",
    )
    ui_color = "#73deff"

    def __init__(
        self,
        *,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),  # this default is a convention to work with the Airflow config
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self._defer = deferrable

    def execute(self, context: Context):
        # turns operator into sensor/deferred operator
        if self.wait_for_completion:
            # Starting the deferral process
            if self._defer:
                self.log.info("Operator in deferrable mode. Starting the deferral process.")
                self.defer(
                    trigger=MyTrigger(poll_interval=self.poke_interval, my_secret_word_1="lemon"),
                    method_name="execute_complete",
                    kwargs={"kwarg_passed_to_execute_complete": "tomato"}
                )
            else: # regular sensor part
                while True:
                    self.log.info("Operator in sensor mode. Polling.")
                    time.sleep(self.poke_interval)
                    import random

                    randint = random.choice([0,2])

                    self.log.info(f"Random number: {randint}")
                    if randint == 2:
                        self.log.info("Result was 2, thats the number! Continuing.")
                        return randint
                    self.log.info("Result was not the one we are waiting for. Sleeping.")

        self.log.info("Not waiting for completion.")

    def execute_complete(self, context: Context, event: tuple[str, dict[str, Any]], kwarg_passed_to_execute_complete: str):
        """Execute when the trigger is complete."""

        self.log.info("Trigger is complete.")
        self.log.info(f"Event: {event}")

        return kwarg_passed_to_execute_complete  # this gets pushed to XCom as `return_value`