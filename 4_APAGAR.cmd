@echo off
title Apagar servicios
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo.ps1" apagar
