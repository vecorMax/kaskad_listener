# Подключаем минимальный набор библиотек для работы
import traceback
import sys
import asyncio
import datetime
# Подключаем библиотечный файл для отправки сообщений на сервер NATS
from smt.rpi.nats.messaging.CServiceMessaging import CServiceMessaging

async def kaskad_listener():
    print(str(datetime.datetime.now()) + "Начало работы программы.")

    # Инициализация методов NATS Server (подключение к серверу + оформление подписки + подключение к БД PostgreSQL)
    messaging = CServiceMessaging()

    # Прием входящих сообщений на NATS Server
    await messaging.receive()

    while 1:
        try:
            print("Current Time: " + str(datetime.datetime.now()))
            await asyncio.sleep(1)
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