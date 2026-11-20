@echo off
title Publicar eventos en Kafka
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo.ps1" publicar
