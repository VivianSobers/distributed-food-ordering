import pytest

from foodpipe.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("foodpipe-tests")
    yield session
    session.stop()
