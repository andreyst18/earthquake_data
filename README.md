# earthquake_data

Проект по загрузке данных по землетрясениям

## Запуск виртуального окружения.

source venv/bin/activate

## API

https://earthquake.usgs.gov/fdsnws/event/1/

- Перед запуском контейнера нужно остановить локальный postgresql:
  остановить - sudo systemctl stop postgresql
  проверить статус - sudo systemctl status postgresql

Работа с docker:
Запуск контейнера: docker-compose up -d
Остановка контейнера: docker-compose stop
