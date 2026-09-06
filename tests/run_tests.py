import sys
import pytest

if __name__ == "__main__":
    # Exécute pytest de manière programmatique sur le fichier de tests avec l'option verbeuse (-v)
    exit_code = pytest.main(["/home/jovyan/work/tests/test_transformers.py", "-v"])
    sys.exit(exit_code)