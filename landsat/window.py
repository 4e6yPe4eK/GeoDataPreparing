import sys
from PyQt5.QtWidgets import (QMainWindow, QWidget, QPushButton, QGridLayout, QLineEdit,
                             QFileDialog, QHBoxLayout, QLabel, QSpinBox)
from PyQt5.QtCore import QThread, QObject, pyqtSignal, Qt
import openpyxl
from widgets.checkboxlistwidget import CheckboxListWidget
from functools import partial
import logging

from landsat import communicator, const

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
        self.setWindowTitle("Landsat")
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

        self.match_line = QLineEdit(self.widget)
        self.match_line.setPlaceholderText("Путь к файлу для сопоставления названий полей")
        self.layout.addWidget(self.match_line, 3, 0, 1, 1)

        self.match_button = QPushButton("Выбрать", self.widget)
        self.match_button.clicked.connect(self.match_button_clicked)
        self.layout.addWidget(self.match_button, 3, 1, 1, 1)

        self.match_hash = 0
        self.match_data = {}

        self.expected_resolution_label = QLabel(self.widget)
        self.expected_resolution_label.setText("Ожидаемое разрешение в метрах")
        self.layout.addWidget(self.expected_resolution_label, 4, 0, 1, 1)
        self.expected_resolution_line = QSpinBox(self.widget)
        self.expected_resolution_line.setRange(1, 1000)
        self.expected_resolution_line.setValue(30)
        self.expected_resolution_line.setSingleStep(10)
        self.layout.addWidget(self.expected_resolution_line, 4, 1, 1, 1)

        self.choice_button_layout = QHBoxLayout()
        self.layout.addLayout(self.choice_button_layout, 5, 0, 1, 2)

        self.field_choice_button = QPushButton("Поля", self.widget)
        self.field_choice_button.clicked.connect(self.field_choice_button_clicked)
        self.choice_button_layout.addWidget(self.field_choice_button)
        self.field_choice_widget = CheckboxListWidget(self.widget)

        self.coefficient_choice_button = QPushButton("Коэффициенты", self.widget)
        self.coefficient_choice_button.clicked.connect(self.coefficient_choice_button_clicked)
        self.choice_button_layout.addWidget(self.coefficient_choice_button)
        self.coefficient_choice_widget = CheckboxListWidget(self.widget)
        self.coefficient_choice_widget.set_choices(choices=const.LANDSAT_COEFFICIENT_NAMES)

        self.start_button = QPushButton("Начать", self.widget)
        self.start_button.clicked.connect(self.start_button_clicked)
        self.layout.addWidget(self.start_button, 6, 0, 1, 2)

    def load_match_data(self):
        match_path = self.match_line.text()
        self.match_hash = hash(match_path)
        self.match_data = {}
        wb: openpyxl.workbook.Workbook = openpyxl.load_workbook(match_path, read_only=True)
        sheet = wb.active
        for row in sheet.rows:
            self.match_data[int(row[0].value)] = str(row[1].value)

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

    def field_choice_button_clicked(self):
        match_path = self.match_line.text()
        if not match_path:
            self.message("Ошибка: файл для сопоставления не выбран", 3000)
            return
        if self.match_hash != hash(match_path):
            self.load_match_data()
            self.field_choice_widget.set_choices(sorted(set(self.match_data.values())))
        self.field_choice_widget.exec()

    def coefficient_choice_button_clicked(self):
        self.coefficient_choice_widget.exec()

    def match_button_clicked(self):
        directory = QFileDialog.getOpenFileName(self, "Выбрать Excel-файл", filter="Excel-файл (*.xls, *.xlsx)")
        self.match_line.setText(directory[0])

    def start_button_clicked(self):
        directory = self.directory_line.text()
        shape = self.shape_line.text()
        output = self.output_line.text()
        fields = self.field_choice_widget.selected_item_texts()
        coefficients = self.coefficient_choice_widget.selected_item_texts()
        match = self.match_line.text()
        expected_resolution = self.expected_resolution_line.value()
        if directory == "":
            self.message("Ошибка: папка с исходными данными не выбрана", 3000)
        elif shape == "":
            self.message("Ошибка: shape-файл не выбран", 3000)
        elif output == "":
            self.message("Ошибка: папка для сохранения не выбрана", 3000)
        elif match == "":
            pass
        else:
            if self.match_hash != hash(match):
                self.load_match_data()
                self.field_choice_widget.set_choices(sorted(set(self.match_data.values())))
                fields = self.field_choice_widget.selected_item_texts()
            data = {
                "directory": directory,
                "shape": shape,
                "output": output,
                "fields": fields,
                "coefficients": coefficients,
                "match_fields": self.match_data,
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
