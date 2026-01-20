import logging
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow

from processor.window import SentinelTab, LandsatTab, MeteorTab, DroneTab, CustomTab
from widgets import ForkWindow


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler('log.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GeoDataPreparing")
        self.resize(900, 600)
        self.setContentsMargins(0, 0, 0, 0)

        self.fork_widget = ForkWindow(self)
        self.fork_widget.add_tab("Sentinel", SentinelTab(self))
        self.fork_widget.add_tab("Landsat", LandsatTab(self))
        self.fork_widget.add_tab("Meteor", MeteorTab(self))
        self.fork_widget.add_tab("Drone", DroneTab(self))
        self.fork_widget.add_tab("Custom", CustomTab(self))
        self.setCentralWidget(self.fork_widget)


if __name__ == "__main__":
    setup_logging()
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
