import logging

from pytest_mock import MockerFixture

from retry_tasks_lib.utils import resolve_callable_from_path


def mock_callable() -> None:
    logging.info("Running mock callable function")


def test_resolve_callable_from_path(mocker: MockerFixture) -> None:
    mock_logger = mocker.patch("tests.test_utils.logging.info")

    path_to_mock_callable = "tests.test_utils.mock_callable"
    callable_handler = resolve_callable_from_path(path_to_mock_callable)
    callable_handler()

    mock_logger.assert_called_once_with("Running mock callable function")
