from PyQt5.QtWidgets import QDialog, QVBoxLayout, QListWidget, QListWidgetItem, QPushButton
from PyQt5.QtCore import Qt, QSignalBlocker


class CheckboxListWidget(QDialog):
    def __init__(self, *args, choices=None, **kwargs):
        super(CheckboxListWidget, self).__init__(*args, **kwargs)
        self.resize(600, 800)

        self.layout = QVBoxLayout(self)

        self.list_widget = QListWidget(self)
        self.layout.addWidget(self.list_widget)
        self.list_widget.itemChanged.connect(self.list_widget_changed)
        self.all_items_list_widget = None
        self.set_choices(choices)

        self.confirm_button = QPushButton("Сохранить", self)
        self.confirm_button.clicked.connect(self.confirm_button_clicked)
        self.layout.addWidget(self.confirm_button)

    def set_choices(self, choices):
        self.list_widget.clear()
        self.all_items_list_widget = QListWidgetItem()
        self.all_items_list_widget.setText("Выбрать все")
        self.list_widget.addItem(self.all_items_list_widget)
        if choices:
            for choice in choices:
                item = QListWidgetItem()
                item.setCheckState(Qt.Unchecked)
                item.setText(choice)
                self.list_widget.addItem(item)
        self.all_items_list_widget.setCheckState(Qt.Checked)

    def list_widget_changed(self, item: QListWidgetItem):
        if item is self.all_items_list_widget:
            state = self.all_items_list_widget.checkState()
            with QSignalBlocker(self.list_widget):
                for i in range(self.list_widget.count()):
                    self.list_widget.item(i).setCheckState(state)
            return
        if item.checkState() == Qt.Checked:
            if all((self.list_widget.item(i).checkState() == Qt.Checked or
                    self.list_widget.item(i) is self.all_items_list_widget for i in range(self.list_widget.count()))):
                with QSignalBlocker(self.list_widget):
                    self.all_items_list_widget.setCheckState(Qt.Checked)
        elif item.checkState() == Qt.Unchecked:
            with QSignalBlocker(self.list_widget):
                self.all_items_list_widget.setCheckState(Qt.Unchecked)

    def selected_item_texts(self):
        ret = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item is self.all_items_list_widget:
                continue
            if item.checkState() == Qt.Checked:
                ret.append(item.text())
        return ret

    def confirm_button_clicked(self):
        self.close()




