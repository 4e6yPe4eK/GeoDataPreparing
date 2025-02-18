import sys
from PyQt5.QtWidgets import QApplication, QMainWindow

from landsat.window import LandsatTab
from meteor.window import MeteorTab
from sentinel.window import SentinelTab
from drone.window import DroneTab
from widgets import ForkWindow


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
        self.setCentralWidget(self.fork_widget)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
