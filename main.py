import sys
from PyQt5.QtWidgets import QApplication
from interface.gui import ClientGUI

def main():
    app = QApplication(sys.argv)
    gui = ClientGUI()
    gui.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()