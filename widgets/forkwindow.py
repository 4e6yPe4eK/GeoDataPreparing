from PyQt5.QtWidgets import QWidget, QPushButton, QHBoxLayout, QVBoxLayout, QStackedWidget
from PyQt5.QtCore import Qt
from PyQt5.uic import loadUi
from pathlib import Path


class ForkWindow(QWidget):
    def __init__(self, parent, *args, **kwargs):
        super(ForkWindow, self).__init__(parent, *args, **kwargs)
        self.setObjectName("ForkWindow")
        path = Path(__file__).parent
        with open(path / "forkwindow.qss", 'r') as style_file:
            self.setStyleSheet(style_file.read())

        self.layout = QHBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)
        self.setLayout(self.layout)

        self.button_widget = QWidget(self)
        self.button_widget.setObjectName("ForkButtonWidget")
        self.button_layout = QVBoxLayout(self.button_widget)
        self.button_layout.setContentsMargins(0, 0, 0, 0)
        self.button_layout.setSpacing(0)
        self.button_layout.addStretch()
        self.layout.addWidget(self.button_widget)

        self.output_widget = QStackedWidget(self)
        self.output_widget.addWidget(QWidget())
        self.layout.addWidget(self.output_widget)

        self.buttons = []

    def add_tab(self, button_name: str, tab: QWidget):
        ind = self.button_layout.count() - 1

        def show_widget():
            self.output_widget.setCurrentIndex(ind + 1)
            for btn in self.buttons:
                btn.setDisabled(False)
            self.sender().setDisabled(True)

        button = QPushButton(button_name, self.button_widget)
        button.clicked.connect(show_widget)
        self.buttons.append(button)
        self.button_layout.insertWidget(ind, button)
        self.output_widget.addWidget(tab)

    def clean(self):
        for btn in self.btns:
            btn.setDisabled(False)
        layout = self.widget.layout()
        while layout.count():
            item = layout.takeAt(0).widget()
            item.setParent(None)
            del item

    def to_info(self):
        self.clean()

        self.btn_info.setDisabled(True)

    def to_stations(self):
        self.clean()

        self.btn_stations.setDisabled(True)
        self.st = StationsWindow(self.widget)
        self.widget.layout().addWidget(self.st)
