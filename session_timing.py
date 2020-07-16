# coding: utf-8

import os
from openpyxl import Workbook

import string
import csv
import datetime
import collections

rawCSVFilePath = unicode(os.getcwd()) + "/.csv"

wb = Workbook()
ws = wb.active

"""
1. Ile było dni z treningiem?
2. Ile dni minęło od pierwszego dnia treningu do ost. dnia treningu?
3. Średni czas gry w dniu
4. Liczba dni w 1/3 treningu
5. Średni czas meczu na 1/3 treningu
6. Proporcja liczby i czasu meczy w 1. 1/3 do 2. 1/3, 2. 1/3 do 3. 1/3
7. Czas treningu w minutach
8. Średni czas odstępu między dniami oraz SD

9. Liczba gier na poziomach trudnosci w czasie - minimalnie (80 gier) i wszystko 
"""
