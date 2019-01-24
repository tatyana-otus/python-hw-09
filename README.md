# MemcLoad
Задание: нужно переделать однопоточную версию memc_load.py в более производительный вариант. Сам скрипт
парсит и заливает в мемкеш поминутную выгрузку логов трекера установленных приложений. Ключом является
тип и идентификатор устройства через двоеточие, значением являет protobuf сообщение.

## Testing
```
python3 -m unittest discover -s tests
```

## Usage
```
memcached -p 33013& memcached -p 33014& memcached -p 33015& memcached -p 33016&
python3 memc_load.py --pattern="tests/data/*.gz"
```

