# -*- coding: utf-8 -*-

import re
import mock
import pytest
from concurrent.futures import ThreadPoolExecutor
from identity.generation import generate, generate_bulk

ID_CHARACTERS = '0ABCDEFG'


@pytest.mark.timeout(6000)
def test_generate_bulk_performance():
    """This needs to run in a clean environment, so if you are using any
    external persistence, clear it before running this."""
    with mock.patch('identity.generation.ID_CHARACTERS', ID_CHARACTERS):
        ids = list(generate_bulk(2097100))
    assert True
