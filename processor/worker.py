from typing import Type, TypeVar

from PyQt5.QtCore import QObject, pyqtSignal

from processor.communicator import AbstractProcessor


T = TypeVar('T', bound=AbstractProcessor)
class Worker(QObject):
    finished = pyqtSignal()
    progressChanged = pyqtSignal(int)
    errorRaised = pyqtSignal(str)
    processor_type: Type[T]

    def __init__(self, processor_type: Type[T], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processor_type = processor_type

    def callback_function(self, *args, callback_type):
        if callback_type == "percent":
            self.progressChanged.emit(args[0])
        if callback_type == "error":
            self.errorRaised.emit(args[0])

    def run(self, data):
        self.progressChanged.emit(0)
        proc: T = self.processor_type(**data, callback=self.callback_function)
        proc.run()
        self.finished.emit()
