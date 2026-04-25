import os
import sys

from PyQt6.QtWidgets import QApplication

from .gui_utils import SettingsManager, ThemeManager
from .parquet_viewer import ParquetViewer


def main():
    app = QApplication(sys.argv)
    app.setOrganizationName("parquet-tool")
    app.setApplicationName("Parquet Tool")
    app.setStyle("Fusion")

    settings = SettingsManager()
    theme = ThemeManager(is_dark=settings.is_dark)
    theme.apply(app)

    viewer = ParquetViewer(theme_manager=theme, settings=settings)

    # open file/directory from CLI argument
    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isfile(path):
            viewer.open_file(path)
        elif os.path.isdir(path):
            viewer.open_directory(path)

    viewer.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
