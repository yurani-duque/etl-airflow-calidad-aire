@echo off
title Preparar demo
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo.ps1" preparar
