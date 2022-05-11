# Retry Task Library

Helpers functions and models to setup async tasks with rq

## Requirements

You'll need to install:

- poetry >= 1.2.0: https://python-poetry.org/docs/master/#installing-with-the-official-installer. 


## Installing libs

- `poetry install -E admin`

## Running tests

- `poetry run pytest tests`

## Bumping release number

- Edit pyproject.toml and alter the version number.
- Push it straight to master with an "increase version no" message
- Then make a new Release in that project.
- Same process as other apps - tag it on the master branch with the new version number.
- That will make the new package and put it on the pypi server
- Then edit the consuming app's Pipfile to the new version number and run `pipenv lock && pipenv sync --dev`


