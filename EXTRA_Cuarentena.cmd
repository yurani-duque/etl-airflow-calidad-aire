@echo off
title Demo de cuarentena
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo.ps1" cuarentena
