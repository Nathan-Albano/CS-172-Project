@echo off
REM This batch file runs the Python Reddit crawler script with required arguments

REM You can modify these variables as needed
set SUBREDDITS="news,worldnews,worldpolitics,politics,newshub,newsandpolitics,futurology"
set SIZE_MB=500
set OUTPUT_DIR="./Reddit_Data"

REM Run the Python script with the specified arguments
python Reddit_Crawler.py --subreddits %SUBREDDITS% --sizeMB %SIZE_MB% --outputDir %OUTPUT_DIR%

REM Pause the command prompt window to see the output
pause