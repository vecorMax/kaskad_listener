# Подключаем минимальный набор библиотек для работы
import traceback
import sys
import minimalmodbus
import asyncio
import logging
import datetime
import psycopg2
import asyncpg
# Подключаем библиотечный файл для отправки сообщений на сервер NATS
from smt.rpi.nats.messaging.CServiceMessaging import CServiceMessaging
# Подключаем библиотечный файл для управления транзакциями через сервер Postgre
from smt.rpi.postgres.connection.CServicePostgre import CServicePostgre

async def kaskad_listener():
    print(str(datetime.datetime.now()) + "Начало работы программы.")

    # Инициализация методов NATS Server (соединение, отправка/передача данных...)
    messaging = CServiceMessaging()

    postgre = CServicePostgre()

    while 1:
        try:

            # logging.info(str(datetime.datetime.now()) + " Reading requested a register from controller...")
            # register_demo = instrument.read_register(545, 0)  # Второй параметр: Если, 1 - 770 -> 77.0
            # logging.info(str(datetime.datetime.now()) + " Successfully read a register from controller!")
            # print(register_demo)
            # await asyncio.ensure_future(messaging.send(register_demo))
            #
            # await asyncio.sleep(1)  # Пауза в 1 секунду
            #
            # cursor = conn.cursor()
            # cursor.execute('INSERT INTO kskd_dm (date_load, time_load, reg_load) VALUES(%s, %s, %s)',
            #                (datetime.date.today(), datetime.datetime.now().timetz(), int(register_demo)))
            # conn.commit()  # <- We MUST commit to reflect the inserted data

        except IOError as e:
            print(str(datetime.datetime.now()) + "Failed to read from instrument. Error: ", str(e))
        except ValueError as e:
            print(str(datetime.datetime.now()) + 'Value error:', str(e))
        except TypeError as e:
            print(str(datetime.datetime.now()) + 'TypeError:', str(e))
        except Exception as e:
            print(str(datetime.datetime.now()) + 'Exception:', str(e))

async def main():
    try:
        await kaskad_listener()
    except KeyboardInterrupt:
        # Выход из программы по нажатию Ctrl+C
        print(str(datetime.datetime.now()) + "Завершение работы Ctrl+C.")
    except Exception as e:
        # Прочие исключения
        print(str(datetime.datetime.now()) + "Ошибка в приложении.")
        # Подробности исключения через traceback
        traceback.print_exc(limit=2, file=sys.stdout)
    finally:
        print(str(datetime.datetime.now()) + "Сброс состояния порта в исходное.")
        # Информируем о завершении работы программы
        print(str(datetime.datetime.now()) + "Программа завершена.")


if __name__ == '__main__':
    asyncio.run(main())