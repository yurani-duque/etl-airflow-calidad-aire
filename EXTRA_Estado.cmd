@echo off
title Estado del sistema
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo.ps1" estado
