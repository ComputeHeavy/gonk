import core

class Log(core.Log):
    def __init__(self):
        super().__init__()
        self.events: list[core.Event] = []

    def add_event(self, event: core.Event):
        self.events.append(event)