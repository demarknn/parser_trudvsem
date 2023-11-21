# parser_trudvsem
Собирает ОГРН организаций, в которых значение поля hiTechComplex = True

# Запуск
- Для запуска нужны библиотеки: requests, pandas, os, logging
- Далее запускается стандартно из IDE

# Результат
После работы получаются файлы:
1. company.csv этот файл скачивается с сайта dataset №18
2. in_all_new_file.csv этот файл содержит только ОГРН из файла company.csv
3. verified_ogrn.csv содержит уже проверенные ОГРН, ОГРН из этого файла повторно не проверяются
4. hiTechComplex.csv файл с нужным результатом, ОГРН у которых hiTechComplex=True
5. ogrn.log содержит историю запусков, кол-во новых ОГРН / кол-во искомых ОГРН

# Особенности
company.csv скачаивается новый при каждом запуске. Остальные файлы обновляются и содержат результат прошлых запусков.

Не работает под windows
