@echo off
echo ========================================
echo    TICKETEA SCRAPER - AIRBAG SHOW
echo ========================================
echo.
echo Iniciando scraper...
echo.

cd /d "%~dp0"

python ticketea_scraper.py

echo.
echo ========================================
echo Scraper completado - Cerrando...
echo ========================================
timeout /t 3 /nobreak >nul
exit
