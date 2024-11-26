import pytest


@pytest.fixture
def crates():
    return {"minimal": "./tests/minimal_crate/"}
