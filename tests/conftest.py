import importlib
import os

import pytest


@pytest.fixture(autouse=True)
def set_log_level():
    os.environ['ABXBUS_LOGGING_LEVEL'] = 'WARNING'
    importlib.import_module('abxbus')
