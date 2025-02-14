import logging
from functools import partial

import openpyxl
from PyQt5.QtCore import QThread, QObject, QLocale, pyqtSignal
from PyQt5.QtWidgets import (QMainWindow, QWidget, QPushButton, QGridLayout, QLineEdit,
                             QFileDialog, QLabel, QSpinBox)
from PyQt5.QtGui import QDoubleValidator, QIntValidator

from drone import communicator, const
from widgets.checkboxlistwidget import CheckboxListWidget

logging.basicConfig(filename="log.log", level=logging.INFO)


class Worker(QObject):
    finished = pyqtSignal()
    progressChanged = pyqtSignal(int)
    errorRaised = pyqtSignal(str)

    def callback_function(self, *args, callback_type="percent"):
        if callback_type == "percent":
            self.progressChanged.emit(args[0])
        if callback_type == "error":
            self.errorRaised.emit(args[0])

    def run(self, data):
        self.progressChanged.emit(0)
        communicator.parse_directories(data, self.callback_function)
        self.finished.emit()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Drone")
        self.resize(800, 600)
        self.message("", 1)
        self.new_thread = None
        self.error_state = False

        self.widget = QWidget(self)
        self.setCentralWidget(self.widget)

        self.layout = QGridLayout(self.widget)
        self.widget.setLayout(self.layout)

        self.directory_line = QLineEdit(self.widget)
        self.directory_line.setPlaceholderText("Путь к папке c исходными данными")
        self.layout.addWidget(self.directory_line, 0, 0, 1, 1)

        self.directory_button = QPushButton("Выбрать", self.widget)
        self.directory_button.clicked.connect(self.directory_button_clicked)
        self.layout.addWidget(self.directory_button, 0, 1, 1, 1)

        self.shape_line = QLineEdit(self.widget)
        self.shape_line.setPlaceholderText("Путь к shape-файлу")
        self.layout.addWidget(self.shape_line, 1, 0, 1, 1)

        self.shape_button = QPushButton("Выбрать", self.widget)
        self.shape_button.clicked.connect(self.shape_button_clicked)
        self.layout.addWidget(self.shape_button, 1, 1, 1, 1)

        self.output_line = QLineEdit(self.widget)
        self.output_line.setPlaceholderText("Путь к папке для сохранения")
        self.layout.addWidget(self.output_line, 2, 0, 1, 1)

        self.output_button = QPushButton("Выбрать", self.widget)
        self.output_button.clicked.connect(self.output_button_clicked)
        self.layout.addWidget(self.output_button, 2, 1, 1, 1)

        self.index_line = QLineEdit(self.widget)
        self.index_line.setPlaceholderText("Индекс поля в shape-файле для расчета")
        self.index_line.setValidator(QIntValidator())
        self.layout.addWidget(self.index_line, 4, 0, 1, 2)

        self.expected_resolution_label = QLabel(self.widget)
        self.expected_resolution_label.setText("Ожидаемое разрешение в метрах")
        self.layout.addWidget(self.expected_resolution_label, 5, 0, 1, 1)
        self.expected_resolution_line = QSpinBox(self.widget)
        self.expected_resolution_line.setRange(1, 1000)
        self.expected_resolution_line.setValue(30)
        self.expected_resolution_line.setSingleStep(10)
        self.layout.addWidget(self.expected_resolution_line, 5, 1, 1, 1)

        self.start_button = QPushButton("Начать", self.widget)
        self.start_button.clicked.connect(self.start_button_clicked)
        self.layout.addWidget(self.start_button, 6, 0, 1, 2)

    def message(self, text, time=0):
        self.statusBar().showMessage(text, time)

    def directory_button_clicked(self):
        directory = QFileDialog.getExistingDirectory(self, "Выбрать папку")
        self.directory_line.setText(directory)

    def output_button_clicked(self):
        directory = QFileDialog.getExistingDirectory(self, "Выбрать папку")
        self.output_line.setText(directory)

    def shape_button_clicked(self):
        directory = QFileDialog.getOpenFileName(self, "Выбрать shape-файл", filter="Shape-файл (*.shp)")
        self.shape_line.setText(directory[0])

    def start_button_clicked(self):
        directory = self.directory_line.text()
        shape = self.shape_line.text()
        output = self.output_line.text()
        index = self.index_line.text()
        expected_resolution = self.expected_resolution_line.value()
        if directory == "":
            self.message("Ошибка: папка с исходными данными не выбрана", 3000)
        elif shape == "":
            self.message("Ошибка: shape-файл не выбран", 3000)
        elif output == "":
            self.message("Ошибка: папка для сохранения не выбрана", 3000)
        elif not index or index == '-':
            self.message("Ошибка: индекс не выбран", 3000)
        else:
            data = {
                "directory": directory,
                "shape": shape,
                "output": output,
                "index": int(index),
                "expected_resolution": expected_resolution,
            }
            self.new_thread = QThread()
            worker = Worker()
            worker.moveToThread(self.new_thread)
            self.new_thread.started.connect(partial(worker.run, data))
            worker.finished.connect(self.new_thread.quit)
            worker.finished.connect(self.finished_function)
            worker.progressChanged.connect(self.progress_changed)
            worker.errorRaised.connect(self.error_raised)
            self.new_thread.finished.connect(worker.deleteLater)
            self.new_thread.finished.connect(self.new_thread.deleteLater)
            self.new_thread.start()
            self.start_button.setText("Идет обработка...")
            self.start_button.setEnabled(False)

    def progress_changed(self, percent):
        self.message(f"Завершено на {percent}%")

    def error_raised(self, message):
        logging.error(message)
        self.error_state = True

    def finished_function(self):
        self.start_button.setText("Начать")
        self.start_button.setEnabled(True)
        if self.error_state:
            self.message("Обработка завершена с ошибками", 3000)
            self.error_state = False
        else:
            self.message("Обработка завершена", 3000)
