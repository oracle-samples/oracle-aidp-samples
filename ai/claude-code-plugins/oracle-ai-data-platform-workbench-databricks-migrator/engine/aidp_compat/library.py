"""AIDP Library Utils - Replacement for dbutils.library"""
import subprocess, sys

class AIDPLibraryUtils:
    def restartPython(self):
        print("[AIDP] Warning: restartPython() is not supported on AIDP.")
        print("[AIDP] Libraries installed via %pip should be available immediately.")

    def install(self, path: str):
        subprocess.check_call([sys.executable, "-m", "pip", "install", path])

    def help(self, method=None):
        print("dbutils.library - AIDP Library Utils")
        print("  restartPython() - No-op on AIDP (not supported)")
        print("  install(path) - Install a Python package")
