@echo off
REM Generate API documentation using pdoc3

echo Generating API documentation...

REM Install pdoc3 if not already installed
pip install pdoc3 --quiet

REM Generate HTML documentation for src module
pdoc --html --output-dir docs --force src

echo.
echo Documentation generated successfully!
echo Open docs/src/index.html in your browser to view the API documentation.
echo.
pause
